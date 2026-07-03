package platform

import "os"

// DirOrHome returns dir when it names an existing directory. Otherwise it
// falls back to the current user's home directory, then to / as a last resort.
func DirOrHome(dir string) string {
	if dir != "" {
		if st, err := os.Stat(dir); err == nil && st.IsDir() {
			return dir
		}
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return home
	}
	return "/"
}
