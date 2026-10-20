package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/core"
	"github.com/firebase/genkit/go/core/api"
	"github.com/firebase/genkit/go/genkit"
	"github.com/firebase/genkit/go/plugins/googlegenai"
	systemonex "github.com/firebase/genkit/go/plugins/systemone/exp"
	"github.com/xavidop/senro"
)

// Two models, two jobs. Gemini writes; jev decides.
const (
	chatModel     = "googleai/gemini-3.8-flash"
	decisionModel = "typesafe/jev-latest"
)

// Ticket is what the ingest flow extracts from a raw support email.
type Ticket struct {
	Customer string   `json:"customer"`
	Company  string   `json:"company,omitempty"`
	Summary  string   `json:"summary"`
	Ask      string   `json:"ask"`
	Facts    []string `json:"facts"`
}

// Decision is the triage verdict, in the shape senro's run parameters need.
type Decision struct {
	Department  string  `json:"department"`
	Confidence  float64 `json:"confidence"`
	Urgent      bool    `json:"urgent"`
	Frustration string  `json:"frustration"`
	Escalate    bool    `json:"escalate"`
	Model       string  `json:"model"`
}

// Params turns the verdict into the flat map senro.When conditions read.
func (d Decision) Params() senro.Params {
	return senro.Params{
		"department": d.Department,
		"urgent":     fmt.Sprint(d.Urgent),
		"escalate":   fmt.Sprint(d.Escalate),
	}
}

// Reply is what the draft flow writes.
type Reply struct {
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// Verdict is what the verify flow decides about a reply.
type Verdict struct {
	Approved     bool     `json:"approved"`
	Addresses    float64  `json:"addresses"`
	Overpromises float64  `json:"overpromises"`
	Invents      float64  `json:"invents"`
	Tone         string   `json:"tone"`
	Reasons      []string `json:"reasons,omitempty"`
}

// --- jev question types ------------------------------------------------------
//
// The options, levels and criteria belong to Go types, so the same question
// means the same thing in every flow that asks it.

type Dept string

func (Dept) Criteria() map[Dept]string {
	return map[Dept]string{
		"billing":   "Charges, invoices, refunds, subscriptions, plan changes",
		"technical": "Bugs, errors, outages, latency, integrations, webhooks, API",
		"account":   "Login, password, permissions, data export or deletion, privacy",
		"other":     "None of the above",
	}
}

type Frustration int

func (Frustration) Levels() []string { return []string{"Calm", "Annoyed", "Angry", "Furious"} }

type Urgent struct{}

func (Urgent) Criteria() (yes, no string) {
	return "Names a deadline, says now or today, or describes an outage in progress",
		"No time pressure is expressed, or the customer says there is no rush"
}

type Triage struct {
	Department  systemonex.Choice[Dept]       `json:"department" jsonschema_description:"Which team should own this ticket?"`
	IsUrgent    systemonex.NoulOf[Urgent]     `json:"is_urgent" jsonschema_description:"Does the ticket communicate time pressure?"`
	Frustration systemonex.Score[Frustration] `json:"frustration" jsonschema_description:"How frustrated is the customer?"`
	NeedsHuman  systemonex.Noul               `json:"needs_human" jsonschema_description:"Does resolving this need a human: a legal threat, a contract or SLA dispute, data loss, or a refund outside the playbook?"`
}

type Overpromise struct{}

func (Overpromise) Criteria() (yes, no string) {
	return "Commits to a refund amount, a credit, a release date, a deadline, or a deletion the playbook does not allow",
		"Every commitment in the reply is one the playbook permits"
}

type Tone int

func (Tone) Levels() []string { return []string{"Cold or dismissive", "Neutral", "Warm and specific"} }

type Review struct {
	Addresses    systemonex.NoulOf[systemonex.NoCriteria] `json:"addresses" jsonschema_description:"Does the reply address what the customer actually asked for?"`
	Overpromises systemonex.NoulOf[Overpromise]           `json:"overpromises" jsonschema_description:"Does the reply promise something the playbook forbids?"`
	Invents      systemonex.Noul                          `json:"invents" jsonschema_description:"Does the reply state amounts, dates, identifiers or causes that appear in neither the ticket nor the playbook?"`
	Tone         systemonex.Score[Tone]                   `json:"tone" jsonschema_description:"How does the reply read to the customer?"`
}

// --- flows -------------------------------------------------------------------

type DraftInput struct {
	Ticket   Ticket   `json:"ticket"`
	Decision Decision `json:"decision"`
	Playbook string   `json:"playbook"`
}

type VerifyInput struct {
	Ticket   Ticket `json:"ticket"`
	Reply    Reply  `json:"reply"`
	Playbook string `json:"playbook"`
}

type Flows struct {
	Ingest *core.Flow[string, Ticket, struct{}]
	Triage *core.Flow[Ticket, Decision, struct{}]
	Draft  *core.Flow[DraftInput, Reply, struct{}]
	Verify *core.Flow[VerifyInput, Verdict, struct{}]
	Brief  *core.Flow[DraftInput, string, struct{}]
}

func defineFlows(g *genkit.Genkit) *Flows {
	f := &Flows{}

	f.Ingest = genkit.DefineFlow(g, "ingestTicket", func(ctx context.Context, raw string) (Ticket, error) {
		t, _, err := genkit.GenerateData[Ticket](ctx, g,
			ai.WithModelName(chatModel),
			ai.WithSystem("You turn raw support emails into a structured ticket. "+
				"Facts are identifiers, amounts, dates and error codes quoted from the email, nothing inferred."),
			ai.WithPrompt("Email:\n\n%s", raw))
		if err != nil {
			return Ticket{}, err
		}
		if t == nil {
			return Ticket{}, fmt.Errorf("ingest: the model returned no ticket")
		}
		return *t, nil
	})

	f.Triage = genkit.DefineFlow(g, "triageTicket", func(ctx context.Context, t Ticket) (Decision, error) {
		// One decision call: the state is the ticket, the questions are the
		// fields of Triage. jev answers all four at once, with probabilities.
		out, resp, err := genkit.GenerateData[Triage](ctx, g,
			ai.WithModelName(decisionModel),
			ai.WithPromptParts(ai.NewDataPart(t)))
		if err != nil {
			return Decision{}, err
		}
		if out == nil {
			return Decision{}, fmt.Errorf("triage: the model returned no answers")
		}
		d := Decision{
			Department:  string(out.Department.Choice),
			Confidence:  out.Department.Confidence,
			Urgent:      out.IsUrgent.Probability >= 0.5,
			Frustration: out.Frustration.Label(),
			Model:       systemonex.ResponseInfo(resp).Model,
		}
		// The thresholds are ours, not the model's. A low-confidence routing
		// decision is not a routing decision; it is a ticket for a human.
		if d.Confidence < 0.5 {
			d.Department = "other"
		}
		d.Escalate = d.Department == "other" ||
			out.NeedsHuman.Probability >= 0.6 ||
			out.Frustration.Level() >= 3
		return d, nil
	})

	f.Draft = genkit.DefineFlow(g, "draftReply", func(ctx context.Context, in DraftInput) (Reply, error) {
		urgency := "The customer is not in a hurry."
		if in.Decision.Urgent {
			urgency = "The customer is under time pressure: acknowledge it and say what happens in the next hours."
		}
		r, _, err := genkit.GenerateData[Reply](ctx, g,
			ai.WithModelName(chatModel),
			ai.WithSystem("You are a support agent for Acme Cloud. Write the reply the customer will receive. "+
				"Follow the playbook strictly. Never state amounts, dates or identifiers that are not in the ticket. "+
				"Plain text, under 180 words, sign as 'Acme Cloud Support'."),
			ai.WithPrompt("Playbook:\n%s\n\n%s\n\nTicket:\n%s", in.Playbook, urgency, asJSON(in.Ticket)))
		if err != nil {
			return Reply{}, err
		}
		if r == nil {
			return Reply{}, fmt.Errorf("draft: the model returned no reply")
		}
		// A model sometimes escapes its newlines inside the JSON string.
		r.Body = strings.ReplaceAll(r.Body, `\n`, "\n")
		return *r, nil
	})

	f.Verify = genkit.DefineFlow(g, "verifyReply", func(ctx context.Context, in VerifyInput) (Verdict, error) {
		// A second decision call, this time as a guardrail. The state is the
		// ticket, the draft and the playbook; the questions are the fields of
		// Review. The flow owns the thresholds and the verdict.
		out, _, err := genkit.GenerateData[Review](ctx, g,
			ai.WithModelName(decisionModel),
			ai.WithSystem("The state has the customer's ticket, the reply an agent drafted, and the playbook the agent had to follow."),
			ai.WithPromptParts(ai.NewDataPart(in)))
		if err != nil {
			return Verdict{}, err
		}
		if out == nil {
			return Verdict{}, fmt.Errorf("verify: the model returned no answers")
		}
		v := Verdict{
			Addresses:    out.Addresses.Probability,
			Overpromises: out.Overpromises.Probability,
			Invents:      out.Invents.Probability,
			Tone:         out.Tone.Label(),
		}
		if v.Addresses < 0.6 {
			v.Reasons = append(v.Reasons, "the reply does not address the customer's ask")
		}
		if v.Overpromises >= 0.5 {
			v.Reasons = append(v.Reasons, "the reply promises something the playbook forbids")
		}
		if v.Invents >= 0.5 {
			v.Reasons = append(v.Reasons, "the reply invents facts")
		}
		if out.Tone.Level() == 0 {
			v.Reasons = append(v.Reasons, "the tone is cold")
		}
		v.Approved = len(v.Reasons) == 0
		return v, nil
	})

	f.Brief = genkit.DefineFlow(g, "escalationBrief", func(ctx context.Context, in DraftInput) (string, error) {
		return genkit.GenerateText(ctx, g,
			ai.WithModelName(chatModel),
			ai.WithSystem("You write a one-screen handoff brief for a senior support engineer who has not read the ticket. "+
				"Markdown with three sections: Situation, Risk, Suggested next action. Under 150 words. No greetings."),
			ai.WithPrompt("Triage: department=%s urgent=%v frustration=%s\n\nTicket:\n%s",
				in.Decision.Department, in.Decision.Urgent, in.Decision.Frustration, asJSON(in.Ticket)))
	})

	return f
}

// --- engine ------------------------------------------------------------------
//
// A func step in a pod runs in a fresh process: senro sends the pipeline
// binary in and re-enters it. So Genkit is initialised lazily, inside the
// step, from the secret files senro delivered to that pod. The API keys
// never appear in the pod spec, the plan, or the logs.

var (
	engineOnce sync.Once
	engine     *Flows
	engineErr  error
)

func flows(ctx senro.Ctx) (*Flows, error) {
	engineOnce.Do(func() {
		var plugins []api.Plugin
		gemini, err := readSecret(ctx, "Gemini")
		if err != nil {
			engineErr = err
			return
		}
		if gemini != "" {
			plugins = append(plugins, &googlegenai.GoogleAI{APIKey: gemini})
		}
		typesafe, err := readSecret(ctx, "TypeSafe")
		if err != nil {
			engineErr = err
			return
		}
		if typesafe != "" {
			ts := systemonex.TypeSafe()
			ts.APIKey = typesafe
			plugins = append(plugins, ts)
		}
		g := genkit.Init(ctx, genkit.WithPlugins(plugins...))
		engine = defineFlows(g)
	})
	return engine, engineErr
}

// readSecret returns the value of a secret this step declared, or "" when
// the step did not declare it. ctx.Secret hands back a file path, never the
// value: the value lives in a 0400 file under /run/senro/secrets in the pod.
func readSecret(ctx senro.Ctx, name string) (string, error) {
	path := ctx.Secret(name)
	if path == "" {
		return "", nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("reading secret %s: %w", name, err)
	}
	return strings.TrimSpace(string(b)), nil
}

// asJSON renders a value for a chat prompt. A chat model takes the ticket as
// text; only a decision model takes it as a data part.
func asJSON(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}
