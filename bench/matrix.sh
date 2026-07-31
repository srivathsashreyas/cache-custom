#!/usr/bin/env bash
# Benchmark matrix: sharding strategies × eviction policies, fair Redis baseline.
# Purpose: shared knobs for local and GKE so comparisons stay consistent.
#
# Sourced by run-local.sh; run-gke.sh uses gen_go_cache_config + same conventions.
# Tenant name: s{strategy}-{policy}  password: BENCH_PASS (default benchpass)
# Same MaxMemory + EvictionPolicy for Redis (when policy is Redis-native) and go_cache.

# Redis maxmemory-policy names we share with stock Redis.
# allkeys-fifo / volatile-fifo are go_cache extensions — go_cache only.
BENCH_REDIS_POLICIES=(
  noeviction
  allkeys-lru
  allkeys-lfu
  allkeys-random
  volatile-lru
  volatile-lfu
  volatile-random
  volatile-ttl
)
BENCH_GO_ONLY_POLICIES=(
  allkeys-fifo
  volatile-fifo
)
BENCH_STRATEGIES=(1 2 3)

# Shared memory budget (bytes) for Redis maxmemory and tenant MaxMemory.
BENCH_MAX_MEMORY="${BENCH_MAX_MEMORY:-268435456}" # 256 MiB
BENCH_SHARD_COUNT="${BENCH_SHARD_COUNT:-4}"
BENCH_PASS="${BENCH_PASS:-benchpass}"

# Smoke: one Redis-native policy × all strategies (wiring + fair baseline without full grid).
BENCH_SMOKE_POLICY="${BENCH_SMOKE_POLICY:-allkeys-lru}"

tenant_name() {
  # Args: strategy policy → s1-allkeys-lru
  echo "s${1}-${2}"
}

is_redis_policy() {
  local p="$1" x
  for x in "${BENCH_REDIS_POLICIES[@]}"; do
    [[ "${x}" == "${p}" ]] && return 0
  done
  return 1
}

# Populate arrays POLICIES_TO_RUN and STRATEGIES_TO_RUN based on smoke flag.
# Args: smoke=true|false
matrix_select() {
  local smoke="${1:-false}"
  STRATEGIES_TO_RUN=("${BENCH_STRATEGIES[@]}")
  if [[ "${smoke}" == "true" ]]; then
    POLICIES_TO_RUN=("${BENCH_SMOKE_POLICY}")
  else
    POLICIES_TO_RUN=("${BENCH_REDIS_POLICIES[@]}" "${BENCH_GO_ONLY_POLICIES[@]}")
  fi
}

# Write go_cache server JSON with one tenant per (strategy, policy) in POLICIES_TO_RUN × STRATEGIES_TO_RUN.
# Args: output_path
gen_go_cache_config() {
  local out="$1"
  local app_id=0 strat pol name first=true

  {
    cat <<EOF
{
  "Security": {
    "Profile": "protected",
    "RequireAuth": true,
    "MaxClients": 10000
  },
  "Persistence": {
    "Mode": "none"
  },
  "Observability": {
    "MetricsAddr": ":9090",
    "LogJSON": false,
    "LogLevel": "warn",
    "LogCommands": false
  },
  "Tenants": [
EOF
    for strat in "${STRATEGIES_TO_RUN[@]}"; do
      for pol in "${POLICIES_TO_RUN[@]}"; do
        app_id=$((app_id + 1))
        name="$(tenant_name "${strat}" "${pol}")"
        if [[ "${first}" == "true" ]]; then
          first=false
        else
          printf ',\n'
        fi
        cat <<EOF
    {
      "Name": "${name}",
      "Password": "${BENCH_PASS}",
      "AppId": ${app_id},
      "MaxMemory": ${BENCH_MAX_MEMORY},
      "EvictionPolicy": "${pol}",
      "MaxTTL": 0,
      "ShardCount": ${BENCH_SHARD_COUNT},
      "ShardingStrategy": ${strat},
      "MaxClients": 0
    }
EOF
      done
    done
    cat <<'EOF'

  ]
}
EOF
  } >"${out}"
}

# Configure Redis maxmemory + policy (fair match to tenant), then FLUSHALL.
# Args: host port policy
redis_apply_policy() {
  local host="$1" port="$2" pol="$3"
  redis-cli -h "${host}" -p "${port}" CONFIG SET maxmemory "${BENCH_MAX_MEMORY}" >/dev/null
  redis-cli -h "${host}" -p "${port}" CONFIG SET maxmemory-policy "${pol}" >/dev/null
  redis-cli -h "${host}" -p "${port}" FLUSHALL >/dev/null
}

# Run fair A/B for every selected policy/strategy.
# Requires: run_matrix from workloads.sh
# Args: redis_host redis_port go_host go_port
run_comparison_matrix() {
  local rh="$1" rp="$2" gh="$3" gp="$4"
  local pol strat user

  echo "matrix max_memory=${BENCH_MAX_MEMORY} shard_count=${BENCH_SHARD_COUNT}"
  echo "policies=${POLICIES_TO_RUN[*]}"
  echo "strategies=${STRATEGIES_TO_RUN[*]}"
  echo

  for pol in "${POLICIES_TO_RUN[@]}"; do
    echo
    echo "########## policy=${pol} ##########"

    if is_redis_policy "${pol}"; then
      echo "----- Redis maxmemory=${BENCH_MAX_MEMORY} maxmemory-policy=${pol} -----"
      redis_apply_policy "${rh}" "${rp}" "${pol}"
      run_matrix "${rh}" "${rp}" "" "" "redis/${pol}"
    else
      echo "----- Redis: skip (policy ${pol} is go_cache-only) -----"
    fi

    for strat in "${STRATEGIES_TO_RUN[@]}"; do
      user="$(tenant_name "${strat}" "${pol}")"
      echo
      echo "----- go_cache strategy=${strat} policy=${pol} tenant=${user} -----"
      run_matrix "${gh}" "${gp}" "${user}" "${BENCH_PASS}" "go_cache/s${strat}/${pol}"
    done
  done
}
