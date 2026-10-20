package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/xavidop/senro"
)

// Every step is a registered Go function that runs one Genkit flow and
// leaves its result as a file in the "case" workspace. senro carries that
// workspace into the pod before the step and back out afterwards.

const workspace = "case"

func init() {
	senro.RegisterFunc("ai/ingest", Ingest)
	senro.RegisterFunc("ai/triage", TriageStep)
	senro.RegisterFunc("ai/draft", Draft)
	senro.RegisterFunc("ai/verify", Verify)
	senro.RegisterFunc("ai/brief", Brief)
	senro.RegisterFunc("ai/collect", Collect)
	senro.RegisterFunc("ai/seed", Seed)
	senro.RegisterFunc("ai/send", Send)
}

type IngestParams struct {
	File string `json:"file"`
}

type NoParams struct{}

type PlaybookParams struct {
	Playbook string `json:"playbook"`
}

type DirParams struct {
	Dir   string   `json:"dir"`
	Files []string `json:"files,omitempty"`
}

// Ingest: the raw email in the workspace in, ticket.json out. Gemini.
func Ingest(ctx senro.Ctx, p IngestParams) error {
	f, err := flows(ctx)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(wsPath(ctx, p.File))
	if err != nil {
		return err
	}
	t, err := f.Ingest.Run(ctx, string(raw))
	if err != nil {
		return err
	}
	fmt.Fprintf(ctx.Stdout(), "ticket from %s: %s\n", t.Customer, t.Summary)
	return writeJSON(ctx, "ticket.json", t)
}

// TriageStep: ticket.json in, decision.json out. jev.
func TriageStep(ctx senro.Ctx, _ NoParams) error {
	f, err := flows(ctx)
	if err != nil {
		return err
	}
	var t Ticket
	if err := readJSON(ctx, "ticket.json", &t); err != nil {
		return err
	}
	d, err := f.Triage.Run(ctx, t)
	if err != nil {
		return err
	}
	fmt.Fprintf(ctx.Stdout(), "%s decided: department=%s (confidence %.2f) urgent=%v frustration=%q escalate=%v\n",
		d.Model, d.Department, d.Confidence, d.Urgent, d.Frustration, d.Escalate)
	return writeJSON(ctx, "decision.json", d)
}

// Draft: ticket.json + decision.json in, reply.json and reply.md out. Gemini.
func Draft(ctx senro.Ctx, p PlaybookParams) error {
	f, err := flows(ctx)
	if err != nil {
		return err
	}
	var in DraftInput
	if err := readJSON(ctx, "ticket.json", &in.Ticket); err != nil {
		return err
	}
	if err := readJSON(ctx, "decision.json", &in.Decision); err != nil {
		return err
	}
	in.Playbook = p.Playbook
	r, err := f.Draft.Run(ctx, in)
	if err != nil {
		return err
	}
	fmt.Fprintf(ctx.Stdout(), "drafted: %s\n", r.Subject)
	if err := writeJSON(ctx, "reply.json", r); err != nil {
		return err
	}
	return writeFile(ctx, "reply.md", "# "+r.Subject+"\n\n"+r.Body+"\n")
}

// Verify: ticket.json + reply.json in, verdict.json out. jev as a guardrail.
// A rejected reply is a failed step, so everything downstream is skipped.
func Verify(ctx senro.Ctx, p PlaybookParams) error {
	f, err := flows(ctx)
	if err != nil {
		return err
	}
	var in VerifyInput
	if err := readJSON(ctx, "ticket.json", &in.Ticket); err != nil {
		return err
	}
	if err := readJSON(ctx, "reply.json", &in.Reply); err != nil {
		return err
	}
	in.Playbook = p.Playbook
	v, err := f.Verify.Run(ctx, in)
	if err != nil {
		return err
	}
	fmt.Fprintf(ctx.Stdout(), "review: addresses=%.2f overpromises=%.2f invents=%.2f tone=%q\n",
		v.Addresses, v.Overpromises, v.Invents, v.Tone)
	if err := writeJSON(ctx, "verdict.json", v); err != nil {
		return err
	}
	if !v.Approved {
		return fmt.Errorf("reply rejected: %v", v.Reasons)
	}
	fmt.Fprintln(ctx.Stdout(), "approved")
	return nil
}

// Brief: a handoff document for a human. Gemini.
func Brief(ctx senro.Ctx, _ NoParams) error {
	f, err := flows(ctx)
	if err != nil {
		return err
	}
	var in DraftInput
	if err := readJSON(ctx, "ticket.json", &in.Ticket); err != nil {
		return err
	}
	if err := readJSON(ctx, "decision.json", &in.Decision); err != nil {
		return err
	}
	brief, err := f.Brief.Run(ctx, in)
	if err != nil {
		return err
	}
	fmt.Fprintln(ctx.Stdout(), brief)
	return writeFile(ctx, "brief.md", brief+"\n")
}

// Collect runs on the coordinator: it copies files out of the workspace into
// a directory on disk, which is how a verdict crosses from one run to the next.
func Collect(ctx senro.Ctx, p DirParams) error {
	if err := os.MkdirAll(p.Dir, 0o755); err != nil {
		return err
	}
	for _, name := range p.Files {
		b, err := os.ReadFile(wsPath(ctx, name))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(p.Dir, name), b, 0o644); err != nil {
			return err
		}
		fmt.Fprintf(ctx.Stdout(), "collected %s\n", name)
	}
	return nil
}

// Seed is Collect in reverse: it puts the previous run's files into this
// run's workspace.
func Seed(ctx senro.Ctx, p DirParams) error {
	for _, name := range p.Files {
		b, err := os.ReadFile(filepath.Join(p.Dir, name))
		if err != nil {
			return err
		}
		if err := os.WriteFile(wsPath(ctx, name), b, 0o644); err != nil {
			return err
		}
	}
	return nil
}

// Send stands in for the helpdesk API call. It runs on the coordinator, so
// the helpdesk credential never has to reach a pod.
func Send(ctx senro.Ctx, p DirParams) error {
	b, err := os.ReadFile(wsPath(ctx, "reply.md"))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(p.Dir, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(p.Dir, "reply.md"), b, 0o644); err != nil {
		return err
	}
	fmt.Fprintf(ctx.Stdout(), "sent to customer:\n\n%s", b)
	return nil
}

// --- helpers -----------------------------------------------------------------

func wsPath(ctx senro.Ctx, name string) string {
	dir, ok := ctx.Workspace(workspace)
	if !ok {
		// Build() guarantees the mount; this only fires on a wiring mistake.
		panic(fmt.Sprintf("step %s does not mount workspace %q", ctx.StepID(), workspace))
	}
	return filepath.Join(string(dir), name)
}

func readJSON(ctx senro.Ctx, name string, v any) error {
	b, err := os.ReadFile(wsPath(ctx, name))
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func writeJSON(ctx senro.Ctx, name string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(ctx, name, string(b)+"\n")
}

func writeFile(ctx senro.Ctx, name, content string) error {
	return os.WriteFile(wsPath(ctx, name), []byte(content), 0o644)
}
