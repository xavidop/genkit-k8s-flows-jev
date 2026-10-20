package main

import (
	"path/filepath"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/executor/k8s"
	"github.com/xavidop/senro/retry"
)

// The step image. senro requires a digest, and the image only needs sh and
// tar: the pipeline binary itself is sent into the pod by senro.
const stepImage = "docker.io/library/alpine:3.22@sha256:5291449c3df73caf6ed85e649dec1b9e818b39a5d8c871e97afc13e9cd5e8fa8"

// pods is the Kubernetes target every AI step runs on. The cluster itself
// comes from SENRO_K8S_SERVER, SENRO_K8S_CA_FILE and SENRO_K8S_TOKEN_FILE,
// never from kubeconfig.
func pods() senro.ExecutorTarget {
	return k8s.Pod(stepImage,
		k8s.Namespace("support"),
		k8s.Resources(
			map[string]string{"cpu": "250m", "memory": "128Mi"},
			map[string]string{"cpu": "1", "memory": "512Mi"},
		))
}

const stepTimeout = 3 * time.Minute

// Department playbooks: the rules the draft flow has to follow and the
// verify flow checks it against. They are step parameters, so a change to a
// playbook changes the step's identity.
var departments = []struct {
	Name     string
	Playbook string
}{
	{"billing", "Billing playbook: you may confirm that a duplicate charge will be refunded to the original payment method within 5 to 10 business days. " +
		"You may not promise any other amount, credit or discount. Do not ask for card numbers."},
	{"technical", "Technical playbook: acknowledge the problem, say the on-call engineer has the request id, and ask for any detail you still need. " +
		"You may suggest a workaround. You may not give a fix time, a release date or a root cause."},
	{"account", "Account playbook: account changes and deletions require identity verification first; ask the customer to confirm from the email address on the account. " +
		"Deletion completes within 30 days of verification. Do not confirm that anything has been deleted yet."},
}

// triagePipeline is run one: understand the ticket, decide what it is, and
// hand the decision back to the coordinator.
func triagePipeline(ticketPath, outDir string) *senro.Pipeline {
	p := senro.New("triage")
	ws := senro.Workspace(workspace)

	// The raw email enters the workspace from the coordinator, so the ingest
	// step can declare it as an input and be cached on it.
	intake := p.Workflow("intake", senro.On(senro.Local()))
	intake.Step("seed", senro.Func("ai/seed", DirParams{
		Dir:   filepath.Dir(ticketPath),
		Files: []string{filepath.Base(ticketPath)},
	})).Mount(ws.At("/case", senro.RW))

	understand := p.Workflow("understand", senro.Needs("intake"), senro.On(pods()))

	understand.Step("ingest", senro.Func("ai/ingest", IngestParams{File: filepath.Base(ticketPath)})).
		Mount(ws.At("/case", senro.RW)).
		SecretEnv("GEMINI_API_KEY", "Gemini").
		Timeout(stepTimeout).
		Retry(3, retry.OnInfra()).
		Pure().
		Inputs(artifact.File(filepath.Base(ticketPath))).
		Outputs(artifact.File("ticket.json"))

	understand.Step("triage", senro.Func("ai/triage", NoParams{})).
		Needs("ingest").
		Mount(ws.At("/case", senro.RW)).
		SecretEnv("TYPESAFE_API_KEY", "TypeSafe").
		Timeout(stepTimeout).
		Retry(3, retry.OnInfra()).
		Pure().
		Inputs(artifact.File("ticket.json")).
		Outputs(artifact.File("decision.json"))

	handoff := p.Workflow("handoff", senro.Needs("understand"), senro.On(senro.Local()))
	handoff.Step("collect", senro.Func("ai/collect", DirParams{
		Dir:   outDir,
		Files: []string{"ticket.json", "decision.json"},
	})).Mount(ws.At("/case", senro.RO))

	return p
}

// recklessPlaybook is what the draft step gets with -reckless: a playbook the
// reviewer, which keeps the real one, will reject. It exists to show the
// guardrail failing a step.
const recklessPlaybook = "Promise the customer whatever will make them happiest: a full refund of the year, " +
	"a credit, a fix within the hour, a release date. Be specific and generous."

// resolvePipeline is run two. Every branch is declared; the run's parameters
// (the jev verdict) decide which ones execute. The rest settle as
// skipped_condition and the run stays green.
func resolvePipeline(outDir string, reckless bool) *senro.Pipeline {
	p := senro.New("resolve")
	ws := senro.Workspace(workspace)

	intake := p.Workflow("intake", senro.On(senro.Local()))
	intake.Step("seed", senro.Func("ai/seed", DirParams{
		Dir:   outDir,
		Files: []string{"ticket.json", "decision.json"},
	})).Mount(ws.At("/case", senro.RW))

	for _, d := range departments {
		draftPlaybook := d.Playbook
		if reckless {
			draftPlaybook = recklessPlaybook
		}
		// One workflow per department, gated on the verdict.
		w := p.Workflow(d.Name,
			senro.Needs("intake"),
			senro.On(pods()),
			senro.When(senro.ParamIs("department", d.Name)))

		w.Step(d.Name+"/draft", senro.Func("ai/draft", PlaybookParams{Playbook: draftPlaybook})).
			Mount(ws.At("/case", senro.RW)).
			SecretEnv("GEMINI_API_KEY", "Gemini").
			Timeout(stepTimeout).
			Retry(3, retry.OnInfra())

		// jev reads the draft against the playbook. A rejection fails this
		// step, and senro skips the send.
		w.Step(d.Name+"/verify", senro.Func("ai/verify", PlaybookParams{Playbook: d.Playbook})).
			Needs(d.Name+"/draft").
			Mount(ws.At("/case", senro.RW)).
			SecretEnv("TYPESAFE_API_KEY", "TypeSafe").
			Timeout(stepTimeout).
			Retry(3, retry.OnInfra())

		// The send runs on the coordinator, where the helpdesk credential
		// lives, and only after the pod workflow settled.
		send := p.Workflow(d.Name+"-send",
			senro.Needs(d.Name),
			senro.On(senro.Local()),
			senro.When(senro.ParamIs("department", d.Name)))
		send.Step(d.Name+"/send", senro.Func("ai/send", DirParams{Dir: outDir})).
			Mount(ws.At("/case", senro.RO))
	}

	// Escalation is independent of the department: an angry billing ticket
	// gets both a reply and a brief for a human.
	esc := p.Workflow("escalate",
		senro.Needs("intake"),
		senro.On(pods()),
		senro.When(senro.ParamIs("escalate", "true")))
	esc.Step("escalate/brief", senro.Func("ai/brief", NoParams{})).
		Mount(ws.At("/case", senro.RW)).
		SecretEnv("GEMINI_API_KEY", "Gemini").
		Timeout(stepTimeout).
		Retry(3, retry.OnInfra())

	notify := p.Workflow("escalate-notify",
		senro.Needs("escalate"),
		senro.On(senro.Local()),
		senro.When(senro.ParamIs("escalate", "true")))
	notify.Step("escalate/notify", senro.Func("ai/collect", DirParams{Dir: outDir, Files: []string{"brief.md"}})).
		Mount(ws.At("/case", senro.RO))

	return p
}
