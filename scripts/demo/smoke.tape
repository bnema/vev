Require vev
Output out/smoke.gif
Set Shell bash
Set FontSize 20
Set Width 1200
Set Height 750
Set TypingSpeed 50ms

Type "vev new smoke"
Enter
Sleep 2s
Screenshot out/01-session.png
Alt+" "
Sleep 1s
Screenshot out/02-palette.png
Escape
Sleep 500ms
Alt+" "
Sleep 500ms
Type "DET"
Enter
Sleep 1.5s
Type "claude --version"
Enter
Sleep 3s
Type "vev kill smoke"
Enter
Sleep 1s
Screenshot out/03-shell.png
Sleep 500ms
