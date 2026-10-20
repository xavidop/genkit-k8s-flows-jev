# Genkit flows on Kubernetes, routed by a decision model

A support-ticket pipeline where every step is a [Genkit](https://genkit.dev) flow,
every flow runs as a pod in Kubernetes through [senro](https://github.com/xavidop/senro),
and the steps that run next are decided by **jev**, a decision model reached through
Genkit's [System One plugin](https://genkit.dev/docs/go/integrations/system-one/).

Two models, two jobs:

| Model | Reached as | Job |
|---|---|---|
| Gemini | `googleai/gemini-3.8-flash` | reads the email, writes the reply, writes the escalation brief |
| jev | `typesafe/jev-latest` | decides the department, urgency, frustration and whether a human is needed; then reviews the draft against the playbook |

## How it is wired

```
run 1: triage                         run 2: resolve (params = jev's verdict)
  intake (local)      seed              intake (local)       seed
  understand (pods)                     billing   (pods)   draft -> verify(jev) -> billing-send   (local)
    ingest   Gemini  -> ticket.json     technical (pods)   draft -> verify(jev) -> technical-send (local)
    triage   jev     -> decision.json   account   (pods)   draft -> verify(jev) -> account-send   (local)
  handoff (local)     collect -> out/   escalate  (pods)   brief                -> escalate-notify (local)
```

senro evaluates `When` conditions at run start, never from a step's output. So the
verdict crosses from run one to run two as **run parameters**, and every branch that
the verdict did not pick settles as `skipped_condition` with the run still green.
Inside a branch, jev is a guardrail: a rejected draft fails `verify`, and `send` is
skipped.

## What you need

- Go 1.27+, Docker, [kind](https://kind.sigs.k8s.io), `kubectl`
- `GEMINI_API_KEY` and `TYPESAFE_API_KEY` in your environment
- optionally the [senro CLI](https://xavidop.github.io/senro/docs/install/) for the terminal UI

## Run it

```sh
scripts/cluster-up.sh            # kind cluster + RBAC + the three files senro reads
source scripts/cluster-env.sh    # SENRO_K8S_SERVER, SENRO_K8S_CA_FILE, SENRO_K8S_TOKEN_FILE

go run . -ticket tickets/double-charge.txt
go run . -ticket tickets/angry-outage.txt              # technical + escalation brief
go run . -ticket tickets/api-500s.txt -reckless        # the reviewer rejects the draft, send is skipped
senro run . -- -ticket tickets/delete-account.txt      # same, with the terminal UI attached
```

Results land in `out/`: `ticket.json`, `decision.json`, `reply.md` and, when a
human is needed, `brief.md`. Every run also leaves `runs/<id>/` with the plan, the
event stream and one log per step.

```sh
scripts/cluster-down.sh
```

## Files

| File | What it holds |
|---|---|
| `flows.go` | the five Genkit flows and the jev question types |
| `steps.go` | the registered `senro.Func` steps that run them |
| `pipeline.go` | the two pipelines and the Kubernetes target |
| `main.go` | the coordinator: run one, read the verdict, run two |
| `k8s/rbac.yaml` | the namespace and the exact permissions senro needs (note `get` on `pods/exec`: the executor speaks WebSocket) |
| `scripts/` | kind cluster up, env, down |
