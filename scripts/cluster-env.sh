# Source this: it exports the variables senro's Kubernetes executor reads.
# senro deliberately ignores kubeconfig, so these are the only way in.
_here=$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")/.." && pwd)
export SENRO_K8S_SERVER=$(cat "$_here/.kind/server")
export SENRO_K8S_CA_FILE=$_here/.kind/ca.crt
export SENRO_K8S_TOKEN_FILE=$_here/.kind/token
unset _here
