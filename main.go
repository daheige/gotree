// Copyright gotree Author. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/8treenet/gotree/helper"
	_ "github.com/garyburd/redigo/redis"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	if input() {
		return
	}
	newProject()
}

func input() bool {
	if !helper.InSlice(os.Args, "input") {
		return false
	}
	gopath := os.Getenv("GOPATH")
	cmd := exec.Command("rm", "-rf", gopath+"/src/github.com/8treenet/gotree/examples")
	cmd.Output()
	if err := os.Mkdir(gopath+"/src/github.com/8treenet/gotree/examples", os.ModePerm); err != nil {
		fmt.Println(err)
		return true
	}
	cmd = exec.Command("cp", gopath+"/src/examples/learning.sql", gopath+"/src/github.com/8treenet/gotree/examples/learning.sql")
	cmd.Output()
	cmd = exec.Command("cp", "-r", gopath+"/src/examples/dao", gopath+"/src/github.com/8treenet/gotree/examples/dao")
	cmd.Output()
	cmd = exec.Command("cp", "-r", gopath+"/src/examples/business", gopath+"/src/github.com/8treenet/gotree/examples/business")
	cmd.Output()
	cmd = exec.Command("cp", "-r", gopath+"/src/examples/protocol", gopath+"/src/github.com/8treenet/gotree/examples/protocol")
	cmd.Output()

	if err := iputGoRecursion(gopath + "/src/github.com/8treenet/gotree/examples/dao"); err != nil {
		fmt.Println(err)
		return true
	}
	if err := iputGoRecursion(gopath + "/src/github.com/8treenet/gotree/examples/business"); err != nil {
		fmt.Println(err)
		return true
	}
	if err := iputGoRecursion(gopath + "/src/github.com/8treenet/gotree/examples/protocol"); err != nil {
		fmt.Println(err)
		return true
	}
	return true
}

func iputGoRecursion(dir string) error {
	flist, e := ioutil.ReadDir(dir)
	if e != nil {
		return e
	}
	for _, f := range flist {
		if f.IsDir() {
			iputGoRecursion(dir + "/" + f.Name())
		} else if strings.Contains(f.Name(), ".go") {
			newfile := strings.Replace(f.Name(), ".go", "._gotree_go", -1)
			cmd := exec.Command("mv", dir+"/"+f.Name(), dir+"/"+newfile)
			cmd.Output()
		}
	}
	return nil
}

func newProject() {
	if !helper.InSlice(os.Args, "new") {
		return
	}
	project := os.Args[len(os.Args)-1]
	if project == "new" {
		return
	}

	gopath := os.Getenv("GOPATH")
	cmd := exec.Command("cp", "-r", gopath+"/src/github.com/8treenet/gotree/examples", gopath+"/src/"+project)
	_, err := cmd.Output()
	if err != nil {
		removeProject(project)
		fmt.Println(err)
	}
	if err = generateGoRecursion(gopath+"/src/"+project, project); err != nil {
		removeProject(project)
		fmt.Println(err)
	}

	cmd = exec.Command("gofmt", "-w", gopath+"/src/"+project)
	_, err = cmd.Output()
	if err != nil {
		removeProject(project)
		fmt.Println(err)
	}
	fmt.Println(fmt.Sprintf("\x1b[0;%dm%s\x1b[0m", 32, "Success in creating "+os.Args[len(os.Args)-1]+"."))
}

func generateGoRecursion(dir string, project string) error {
	flist, e := ioutil.ReadDir(dir)
	if e != nil {
		return e
	}
	for _, f := range flist {
		if f.IsDir() {
			generateGoRecursion(dir+"/"+f.Name(), project)
		} else if strings.Contains(f.Name(), "._gotree_go") {
			generate(dir+"/"+f.Name(), project)
		}
	}
	return nil
}

func generate(file string, project string) error {
	if !strings.Contains(file, "._gotree_go") {
		return nil
	}
	fileData, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	if err = os.Remove(file); err != nil {
		return err
	}

	new := strings.Replace(string(fileData), "examples", project, -1)
	newfile := strings.Replace(file, "._gotree_go", ".go", -1)
	return ioutil.WriteFile(newfile, []byte(new), os.FileMode(0664))
}

func removeProject(project string) {
	gopath := os.Getenv("GOPATH")
	cmd := exec.Command("rm", "-rf", gopath+"/src/"+project)
	cmd.Output()
}
