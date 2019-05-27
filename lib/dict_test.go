package lib

import "testing"

func TestDict(t *testing.T) {
	dict := new(Dict).Gotree()
	var info struct {
		Age  int
		Name string
	}
	info.Age = 25
	info.Name = "Tom"
	dict.Set("user", info)

	var info2 struct {
		Age  int
		Name string
	}
	dict.Get("user", &info2)
	t.Log(info2)
	return
}
