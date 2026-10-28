package archive

// SetBeforeCatalog installs f to run in Commit after the segment and index
// writes and before the catalog write (with c.mu not held).
func SetBeforeCatalog(c *Committer, f func()) { c.beforeCatalog = f }
