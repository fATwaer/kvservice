package raftkv


type Snapshot struct {
	StateMap map[string] string
	commitMap map[Namespace] int
	LastIncludeIndex int
	LastIncludeTerm int
}
