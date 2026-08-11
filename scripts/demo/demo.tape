Require vev
Output out/demo.gif
Set Shell bash
Set FontFamily "JetBrainsMono Nerd Font"
Set FontSize 20
Set Width 1200
Set Height 750
Set Padding 12
Set Margin 20
Set MarginFill "#161B22"
Set WindowBar Colorful
Set WindowBarSize 36
Set BorderRadius 10
Set Theme "GitHub Dark"
Set TypingSpeed 100ms
Set Framerate 30
Set WaitTimeout 30s

# Visible shell commands are typed with hand-varied Type@<speed>
# (70-160ms) and short hesitations at word boundaries so the rhythm
# reads irregular/human; palette codes (CNS/SSP/…) stay single-speed.

# ── Local and remote history, shown immediately ──────────
Type@180ms "vev"
Sleep 600ms
Enter
Sleep 1.1s
# Keep the initial local shell so the final detach/attach cycle is meaningful.
Alt+" "
Sleep 550ms
Type "RNS "
Type@95ms "local"
Enter
Sleep 1s

# The picker starts with the local shell plus two ready remote sessions.
Alt+" "
Sleep 550ms
Type "SSP"
Enter
Sleep 1.5s
Down 2
Sleep 700ms
Enter
Sleep 2.5s

# The selected work session belongs to the remote container.
Type@85ms "eza "
Sleep 200ms
Type@75ms "-la "
Type@130ms "/etc"
Enter
Sleep 1.5s
Alt+" "
Sleep 550ms
Type "SPR"
Enter
Sleep 650ms
Type@80ms "tick"
Type@150ms "er"
Enter
Sleep 1.5s

# ── Session "agents": launch the bell job early so its
#    sleep overlaps the middle scenes. ─────────────────────
Alt+" "
Sleep 550ms
Type "CNS "
Type@95ms "agents"
Enter
Sleep 900ms
Type@80ms "sleep "
Type@140ms "10"
Sleep 220ms
Type@85ms " && printf "
Type@130ms `'\a'`
Enter
Sleep 1s

# Back to work — palette-typed session switch: type the session
# name directly (no command code), it ranks top of the results.
Alt+" "
Sleep 550ms
Type@95ms "work"
Enter
Sleep 1s

# ── Session "work": stacked panes (ticker keeps ticking) ─
# Member 1 is the eza pane already on screen; member 2 opens the
# ticker script itself in neovim (self-referential, bash syntax).
Alt+h
Sleep 550ms
Alt+" "
Sleep 550ms
Type "STP"
Enter
Sleep 650ms
Type@85ms "nvim "
Sleep 150ms
Type@110ms "/usr/local/bin/"
Type@130ms "ticker"
Enter
Sleep 1.5s
Alt+k
Sleep 900ms
Alt+j
Sleep 900ms

# ── Session "work": floating pane preserves its program ──
# Open a man page, scroll it, hide, then show again at the same
# scroll position — proves the float keeps program + scroll state.
Alt+f
Sleep 550ms
Type@90ms "man "
Type@130ms "ls"
Enter
Sleep 1.2s
Space
Sleep 700ms
Alt+f
Sleep 900ms
Alt+f
Sleep 1.2s
Type "q"
Sleep 550ms
Alt+f
Sleep 500ms

# ── Session "monitor": fastfetch + htop, each full-width,
#    never split. ─────────────────────────────────────────
Alt+" "
Sleep 550ms
Type "CNS "
Type@95ms "monitor"
Enter
Sleep 900ms
Type@80ms "fast"
Type@130ms "fetch"
Enter
Sleep 1.7s
Alt+" "
Sleep 550ms
Type "CNT"
Enter
Sleep 550ms
Type@90ms "htop"
Enter
Sleep 1.9s
Alt+"1"
Sleep 1.2s
Alt+"2"
Sleep 1.1s

# ── Bell cross-session: the agents job has rung; jump to it ─
Sleep 900ms
Alt+a
Sleep 1.9s

# ── Session picker now lists local and remote sessions ───
Alt+" "
Sleep 550ms
Type "SSP"
Enter
Sleep 1.5s
Escape
Sleep 650ms

# ── Detach / re-attach: local sessions survive, ticker on ─
Alt+" "
Sleep 550ms
Type "DET"
Enter
Sleep 1s
Type@90ms "vev "
Type@75ms "ls "
Type@120ms "--all"
Enter
Sleep 1.8s
Type@90ms "vev "
Sleep 150ms
Type@80ms "attach "
Type@140ms "local"
Enter
Sleep 2.2s
Sleep 400ms
