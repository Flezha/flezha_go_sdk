package flezha_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/flezha/flezha_go_sdk/internal/contract"
)

// exported is every exported name of the package in dir, prefixed as the
// fixture spells it: `Func`, `Type`, `Type.Method`, `Type.Field`, and
// `hook:Method` for a method of the hook interfaces a tasklet implements.
func exported(t *testing.T, dir, prefix string) map[string]bool {
	t.Helper()
	pkgs, err := parser.ParseDir(token.NewFileSet(), dir, func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				switch d := decl.(type) {
				case *ast.FuncDecl:
					if d.Recv == nil {
						names[prefix+d.Name.Name] = d.Name.IsExported()
						continue
					}
					names[prefix+receiver(d.Recv.List[0].Type)+"."+d.Name.Name] = d.Name.IsExported()
				case *ast.GenDecl:
					for _, spec := range d.Specs {
						ts, ok := spec.(*ast.TypeSpec)
						if !ok {
							continue
						}
						names[prefix+ts.Name.Name] = ts.Name.IsExported()
						members(ts, prefix, names)
					}
				}
			}
		}
	}
	for name, public := range names {
		if !public {
			delete(names, name)
		}
	}
	return names
}

func members(ts *ast.TypeSpec, prefix string, names map[string]bool) {
	switch body := ts.Type.(type) {
	case *ast.StructType:
		for _, f := range body.Fields.List {
			for _, n := range f.Names {
				names[prefix+ts.Name.Name+"."+n.Name] = ts.Name.IsExported() && n.IsExported()
			}
		}
	case *ast.InterfaceType:
		hook := strings.HasSuffix(ts.Name.Name, "Hook")
		for _, m := range body.Methods.List {
			for _, n := range m.Names {
				if hook {
					names["hook:"+n.Name] = true
				} else {
					names[prefix+ts.Name.Name+"."+n.Name] = ts.Name.IsExported()
				}
			}
		}
	}
}

// receiver is a method's receiver type name, without pointer or type parameters.
func receiver(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.StarExpr:
		return receiver(e.X)
	case *ast.IndexExpr:
		return receiver(e.X)
	case *ast.IndexListExpr:
		return receiver(e.X)
	case *ast.Ident:
		return e.Name
	}
	return ""
}

func TestEveryPublicNameIsTheFixtures(t *testing.T) {
	text, err := contract.Read("test/fixtures/sdk_public_names.txt")
	if err != nil {
		t.Fatal(err)
	}
	names := exported(t, ".", "")
	for name := range exported(t, "flezhatest", "flezhatest.") {
		names[name] = true
	}
	for _, line := range strings.Split(string(text), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		row := strings.Split(line, "|")
		const goColumn = 8
		if len(row) <= goColumn {
			t.Fatalf("row %q has no go column", row[0])
		}
		for _, name := range strings.Fields(row[goColumn]) {
			switch {
			case name == "-":
			case strings.HasPrefix(name, "!"):
				if names[name[1:]] {
					t.Errorf("%s: %s must not be public", row[0], name[1:])
				}
			case !names[name]:
				t.Errorf("%s: %s is not a public name of the Go SDK", row[0], name)
			}
		}
	}
}
