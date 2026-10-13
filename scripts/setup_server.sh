#!/usr/bin/env bash
# First-time setup of the public job board's server (Ubuntu VPS).
#
#   sudo bash scripts/setup_server.sh
#
# Installs Docker if missing, asks for the secrets and writes them to .env
# (readable by root only), builds the Chrome base image for THIS machine and
# starts the scraper (every 3 minutes, Wuzzuf by default) and the sync service
# that publishes new jobs to Turso. Safe to re-run: an existing .env is kept
# unless you choose to replace it.
set -euo pipefail
umask 077  # .env (and its rewrites) readable by root only
cd "$(dirname "$0")/.."

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
die() { printf '\nERROR: %s\n' "$*" >&2; exit 1; }

[ "$(id -u)" -eq 0 ] || die "run it with sudo: sudo bash scripts/setup_server.sh"

ask() {  # ask VAR "question" [secret]
    local value=""
    while [ -z "$value" ]; do
        if [ "${3:-}" = secret ]; then
            read -r -s -p "$2: " value; echo
        else
            read -r -p "$2: " value
        fi
        value="${value//[[:space:]]/}"
    done
    printf -v "$1" '%s' "$value"
}

set_env() {  # set_env KEY VALUE: replace KEY=... in .env or append it
    KEY="$1" VALUE="$2" awk '
        BEGIN { k = ENVIRON["KEY"]; v = ENVIRON["VALUE"] }
        index($0, k "=") == 1 { if (!done) print k "=" v; done = 1; next }
        { print }
        END { if (!done) print k "=" v }' .env > .env.tmp
    mv .env.tmp .env
}

say "Docker"
if ! command -v docker >/dev/null 2>&1; then
    curl -fsSL https://get.docker.com | sh
fi
docker compose version >/dev/null 2>&1 || die "docker compose plugin missing"
echo "ok: $(docker --version)"

write_env=yes
if [ -f .env ]; then
    read -r -p ".env already exists. Replace it? (y/N): " answer
    [ "${answer,,}" = y ] || write_env=no
fi

if [ "$write_env" = yes ]; then
    say "Secrets (typed text is hidden for tokens)"
    ask turso_url "Turso database URL (libsql://...)"
    case "$turso_url" in libsql://*|https://*) ;; *) die "Turso URL must start with libsql://" ;; esac
    ask turso_token "Turso token (Read & Write)" secret
    ask tg_token "Telegram bot token (from @BotFather)" secret
    ask tg_channel "Telegram channel for new jobs (@channelname or -100... id)"
    ask tg_me "Your own Telegram chat id for error alerts (from @userinfobot)"

    cp .env.example .env
    chmod 600 .env
    set_env TELEGRAM_TOKEN "$tg_token"
    set_env TELEGRAM_CHAT_ID "$tg_channel"
    set_env TELEGRAM_FAILURE_CHAT_ID "$tg_me"
    set_env TURSO_DATABASE_URL "$turso_url"
    set_env TURSO_AUTH_TOKEN "$turso_token"
    # Public site: LinkedIn's terms forbid republishing its listings.
    set_env LINKEDIN_ENABLED false
    set_env WUZZUF_ENABLED true
    echo "wrote .env (root only)"
fi

# ./markup is bind-mounted into the scraper, which runs as uid 1000 and saves
# page snapshots there; a clone made as root would refuse those writes.
chown -R 1000:1000 markup

say "Chrome base image (a few minutes)"
built=$(scripts/build_chrome_base.sh | sed -n 's/^Built \(rtjobs-chrome-base:[^ ]*\).*/\1/p')
[ -n "$built" ] || die "could not build the Chrome base image"
set_env CHROME_BASE "$built"
echo "using $built"

say "Starting scraper + scheduler + sync"
docker compose up -d --build

say "Done"
docker compose ps
cat <<'EOF'

Next:
  docker compose logs -f sync     # "sync: N new jobs" after the first scrape (~3 min)
  docker compose logs ofelia      # scraper runs every 3 minutes
Then add TURSO_DATABASE_URL + the READ-ONLY Turso token in Vercel and redeploy.
EOF
