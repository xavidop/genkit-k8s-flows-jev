// Command genkit-k8s-flows-jev triages a support ticket with Genkit flows
// running as senro steps in Kubernetes pods. Gemini reads and writes; jev, a
// decision model reached through Genkit's System One plugin, decides which
// steps run next.
//
//	go run . -ticket tickets/double-charge.txt
//
// or, to build, run and attach a terminal UI in one go:
//
//	senro run . -- -ticket tickets/double-charge.txt
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro"
	"github.com/xavidop/senro/attach"
)

// Secrets is resolved once on the coordinator. Each step names the field it
// needs, and senro delivers it to the pod as a file.
type Secrets struct {
	Gemini   secret.String `source:"env:GEMINI_API_KEY"`
	TypeSafe secret.String `source:"env:TYPESAFE_API_KEY"`
}

func main() {
	ctx := context.Background()

	// Inside a pod, senro re-enters this same binary to run a Func step.
	// That has to be handled before flag parsing sees senro's arguments.
	if handled, err := senro.StepChild(ctx); handled {
		if err != nil {
			log.Fatal(err)
		}
		return
	}

	ticketPath := flag.String("ticket", "tickets/double-charge.txt", "raw ticket to process")
	outDir := flag.String("out", "out", "where the verdict and the reply land")
	reckless := flag.Bool("reckless", false, "draft with a playbook the reviewer will reject, to see the guardrail fire")
	flag.Parse()

	ticket, err := filepath.Abs(*ticketPath)
	if err != nil {
		log.Fatal(err)
	}
	out, err := filepath.Abs(*outDir)
	if err != nil {
		log.Fatal(err)
	}
	secrets, err := mamori.Load[Secrets](ctx)
	if err != nil {
		log.Fatal(err)
	}

	// run runs one pipeline with an attach socket, so `senro run` and
	// `senro attach` can render it live.
	run := func(p *senro.Pipeline, extra ...senro.Option) error {
		att, err := attach.Listen(ctx, attach.Options{Bind: attach.AutoUnixSocket, Pipeline: p.Name()})
		if err != nil {
			return err
		}
		defer func() { _ = att.Close() }()
		opts := append([]senro.Option{
			senro.WithAttach(att),
			senro.WithSecrets(secrets),
			// The pods are Linux and this coordinator may not be, so senro
			// cross-compiles this package for every func step in a pod.
			senro.WithFuncBuild("."),
		}, extra...)
		return senro.Run(ctx, p, opts...)
	}

	// Run one: understand and decide.
	if err := run(triagePipeline(ticket, out)); err != nil {
		exit(err)
	}

	var decision Decision
	b, err := os.ReadFile(filepath.Join(out, "decision.json"))
	if err != nil {
		log.Fatal(err)
	}
	if err := json.Unmarshal(b, &decision); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\njev routed the ticket: %s\n\n", string(b))

	// Run two: act. The verdict is the run's parameters, and senro.When
	// prunes every branch the verdict did not pick before anything starts.
	if err := run(resolvePipeline(out, *reckless), senro.WithParams(decision.Params())); err != nil {
		exit(err)
	}
}

func exit(err error) {
	var runErr *senro.RunError
	if errors.As(err, &runErr) {
		fmt.Fprintf(os.Stderr, "run ended: %s\n", runErr.Status)
		for _, s := range runErr.Steps {
			fmt.Fprintf(os.Stderr, "  %s %s (exit %d)\n", s.ID, s.State, s.ExitCode)
		}
		fmt.Fprintf(os.Stderr, "  evidence: %s/events.jsonl\n", runErr.Dir)
	} else {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(1)
}
