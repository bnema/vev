package agentactivity

import "testing"

func TestIdentify(t *testing.T) {
	tests := []struct {
		name string
		argv []string
		comm string
		want Agent
	}{
		{"native claude", []string{"claude"}, "claude", Claude},
		{"claude version path", []string{"/home/u/.local/share/claude/versions/2.1.308", "--resume"}, "2.1.308", Claude},
		{"claude-code package", []string{"node", "/usr/lib/node_modules/@anthropic-ai/claude-code/cli.js"}, "node", Claude},
		{"node codex wrapper", []string{"node", "/home/u/.bun/bin/codex"}, "node", Codex},
		{"node codex.js", []string{"/usr/bin/node", "--no-warnings", "/home/u/.bun/install/global/node_modules/@openai/codex/bin/codex.js"}, "node", Codex},
		{"bun codex", []string{"bun", "run", "codex"}, "bun", Codex},
		{"native codex child", []string{"/x/vendor/x86_64-unknown-linux-musl/bin/codex", "resume"}, "codex", Codex},
		{"bash -c claude", []string{"bash", "-c", "claude --resume"}, "bash", Claude},
		{"sh -lc with env", []string{"/bin/sh", "-lc", "FOO=1 exec opencode ."}, "sh", OpenCode},
		{"npx codex", []string{"npx", "-y", "@openai/codex@latest"}, "npx", Codex},
		{"env wrapper", []string{"/usr/bin/env", "TERM=xterm", "zut"}, "env", Zut},
		{"opencode", []string{"/home/u/.opencode/bin/opencode"}, "opencode", OpenCode},
		{"zut", []string{"zut", "--model", "gpt-5.4"}, "zut", Zut},
		{"versioned binary", []string{"codex-0.163.0"}, "codex-0.163.0", Codex},
		{"uppercase", []string{"/opt/Claude"}, "Claude", Claude},
		{"vim", []string{"vim", "claude.md"}, "vim", None},
		{"shell", []string{"fish"}, "fish", None},
		{"shell -c non agent", []string{"bash", "-c", "make test"}, "bash", None},
		{"node script", []string{"node", "server.js"}, "node", None},
		{"node repl", []string{"node"}, "node", None},
		{"comm fallback", nil, "opencode", OpenCode},
		{"comm fallback unknown", nil, "less", None},
		{"empty", nil, "", None},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Identify(tt.argv, tt.comm); got != tt.want {
				t.Fatalf("Identify(%q, %q) = %s, want %s", tt.argv, tt.comm, got, tt.want)
			}
		})
	}
}

// TestIdentifyPackageRunners uses the argv the kernel reports: npx, pnpm,
// yarn, and npm are node scripts, so argv[0] is node.
func TestIdentifyPackageRunners(t *testing.T) {
	tests := []struct {
		name string
		argv []string
		want Agent
	}{
		{"node npx", []string{"node", "/usr/bin/npx", "-y", "@openai/codex"}, Codex},
		{"node npx-cli", []string{"/usr/bin/node", "/usr/lib/node_modules/npm/bin/npx-cli.js", "@anthropic-ai/claude-code"}, Claude},
		{"node npm exec", []string{"node", "/usr/bin/npm", "exec", "--yes", "@openai/codex"}, Codex},
		{"node npm-cli x", []string{"node", "/usr/lib/node_modules/npm/bin/npm-cli.js", "x", "opencode-ai"}, OpenCode},
		{"node pnpm dlx", []string{"node", "/home/u/.local/share/pnpm/pnpm", "dlx", "@anthropic-ai/claude-code"}, Claude},
		{"node pnpx", []string{"node", "/home/u/.local/share/pnpm/pnpx", "@openai/codex"}, Codex},
		{"node yarn dlx", []string{"node", "/usr/lib/node_modules/yarn/bin/yarn.js", "dlx", "-q", "@openai/codex"}, Codex},
		{"bunx", []string{"/home/u/.bun/bin/bunx", "opencode-ai@latest"}, OpenCode},
		{"bun x", []string{"bun", "x", "@anthropic-ai/claude-code"}, Claude},
		{"node npm install", []string{"node", "/usr/bin/npm", "install", "@openai/codex"}, None},
		{"node npx unknown package", []string{"node", "/usr/bin/npx", "prettier", "--write", "."}, None},
		{"node npx without package", []string{"node", "/usr/bin/npx"}, None},
		{"npx opencode-ai", []string{"npx", "opencode-ai"}, OpenCode},
		{"npx claude-code package", []string{"npx", "@anthropic-ai/claude-code@2.1.308"}, Claude},
		{"opencode-ai package script", []string{"node", "/x/node_modules/opencode-ai/bin/opencode"}, OpenCode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Identify(tt.argv, "node"); got != tt.want {
				t.Fatalf("Identify(%q) = %s, want %s", tt.argv, got, tt.want)
			}
		})
	}
}

func TestIdentifyShellScripts(t *testing.T) {
	tests := []struct {
		name string
		argv []string
		want Agent
	}{
		{"cd and", []string{"sh", "-c", "cd x && claude"}, Claude},
		{"or fallback", []string{"bash", "-c", "test -f .env || codex"}, Codex},
		{"semicolons", []string{"bash", "-lc", "source .venv/bin/activate; export A=1; opencode ."}, OpenCode},
		{"trailing semicolon", []string{"sh", "-c", "cd x && zut;"}, Zut},
		{"newline", []string{"sh", "-c", "cd x\nexec claude --resume\n"}, Claude},
		{"agent first", []string{"sh", "-c", "claude && make test"}, None},
		{"pipe is one command", []string{"sh", "-c", "codex | tee log"}, Codex},
		// Quotes are not parsed, so a nested quoted script is not unwrapped.
		{"nested quoted shell", []string{"sh", "-c", "cd x && bash -c 'npx -y @openai/codex'"}, None},
		{"exec -a", []string{"sh", "-c", "cd x && exec -a agent claude"}, Claude},
		{"options before -c", []string{"bash", "-o", "pipefail", "-c", "claude"}, Claude},
		{"fish init command", []string{"fish", "-C", "set x 1", "-c", "codex"}, Codex},
		{"script operand ends options", []string{"bash", "./run.sh", "-c", "claude"}, None},
		{"double dash ends options", []string{"bash", "--", "run.sh", "-c", "claude"}, None},
		{"empty script", []string{"sh", "-c", " ; "}, None},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Identify(tt.argv, ""); got != tt.want {
				t.Fatalf("Identify(%q) = %s, want %s", tt.argv, got, tt.want)
			}
		})
	}
}

func TestIdentifyNestedWrappers(t *testing.T) {
	tests := []struct {
		name string
		argv []string
		want Agent
	}{
		{"env chain", []string{"env", "env", "nohup", "claude"}, Claude},
		{"env then shell", []string{"env", "A=1", "bash", "-c", "env B=2 npx codex"}, Codex},
		{"shell without command", []string{"bash", "-l"}, None},
		{"wrapper only", []string{"env", "A=1"}, None},
		{"uv run", []string{"uv", "run", "--with", "rich", "claude"}, Claude},
		{"uv without run", []string{"uv", "pip", "install", "claude"}, None},
		{"mise x", []string{"mise", "x", "node@22", "--", "codex"}, Codex},
		{"mise exec", []string{"mise", "exec", "--", "opencode"}, OpenCode},
		{"pnpm dlx", []string{"pnpm", "dlx", "@openai/codex"}, Codex},
		{"pnpm install", []string{"pnpm", "install", "codex"}, None},
		{"yarn dlx", []string{"yarn", "dlx", "-q", "@anthropic-ai/claude-code"}, Claude},
		{"npm exec", []string{"npm", "exec", "--yes", "@openai/codex"}, Codex},
		{"npm x", []string{"npm", "x", "opencode"}, OpenCode},
		{"npm install", []string{"npm", "install", "codex"}, None},
		{"nice", []string{"nice", "-n", "10", "claude"}, Claude},
		{"timeout", []string{"timeout", "-s", "INT", "1h", "codex"}, Codex},
		{"timeout duration only", []string{"timeout", "30m"}, None},
		{"sudo", []string{"sudo", "-u", "agent", "-E", "zut"}, Zut},
		{"direnv exec", []string{"direnv", "exec", ".", "claude"}, Claude},
		{"direnv exec without command", []string{"direnv", "exec", "/repo"}, None},
		{"node require value", []string{"node", "-r", "dotenv/config", "/x/@openai/codex/bin/codex.js"}, Codex},
		{"node import value", []string{"node", "--import", "tsx", "/x/@anthropic-ai/claude-code/cli.js"}, Claude},
		{"node eval is not a script", []string{"node", "-e", "require('claude')"}, None},
		{"env -u in shell", []string{"bash", "-c", "env -u FOO claude"}, Claude},
		{"command -p in shell", []string{"sh", "-c", "command -p codex"}, Codex},
		{"nohup env in shell", []string{"sh", "-c", "nohup env -C /tmp A=1 opencode"}, OpenCode},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Identify(tt.argv, ""); got != tt.want {
				t.Fatalf("Identify(%q) = %s, want %s", tt.argv, got, tt.want)
			}
		})
	}
}
