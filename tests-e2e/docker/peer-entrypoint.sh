#!/bin/sh
# Apply firewall/NAT rules BEFORE the engine binary starts.
#
# The harness used to exec iptables into the container after start() —
# but the engine boots (and with mDNS on a shared bridge, discovers and
# dials peers) the instant the container starts, and any flow
# established before the rules land is grandfathered forever by the
# conntrack ESTABLISHED accept. Applying the rules here closes that
# race by construction, and re-applies them on container restart
# (a fresh network namespace wipes rules; the env var survives).
#
# IPTABLES_RULES: newline-separated iptables argument strings, e.g.
#   -A INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
# A failing rule fails container startup loudly (set -e) rather than
# silently running the scenario without its NAT shape.
set -e
if [ -n "$IPTABLES_RULES" ]; then
    printf '%s\n' "$IPTABLES_RULES" | while IFS= read -r rule; do
        [ -n "$rule" ] || continue
        # Word-splitting is intentional: each line is a pre-tokenized
        # argument string owned by the harness.
        # shellcheck disable=SC2086
        iptables $rule
    done
fi
exec /usr/local/bin/test-peer "$@"
