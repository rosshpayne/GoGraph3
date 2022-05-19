package parser

import (
	"strings"
	"testing"
)

func compare(doc, expected string) bool {

	if strings.Compare(trimWS(doc), trimWS(expected)) == 0 {
		return false
	}

	return true

}

func diffPos(doc, expected string) int {

	te := trimWS(expected)
	td := trimWS(doc)
	for i, _ := range td {
		if len(te) > i {
			if te[i] != td[i] {
				return i
			}
		} else {
			return i
		}
	}
	return 0
}

// trimWS trims whitespaces from input string. Objective is to compare strings real content - not influenced by whitespaces
func trimWS(input string) string {

	var out strings.Builder
	for _, v := range input {
		if !(v == '\u0009' || v == '\u0020' || v == '\u000A' || v == '\u000D' || v == ',') {
			out.WriteRune(v)
		}
	}
	return out.String()

}

// checkErrors compares actual errors from test against slice of expected errors
func checkErrors(errs []error, expectedErr []string, t *testing.T) {

	for _, ex := range expectedErr {
		if len(ex) == 0 {
			break
		}
		found := false
		for _, err := range errs {
			if trimWS(err.Error()) == trimWS(ex) {
				found = true
			}
		}
		if !found {
			t.Errorf(`Expected Error = [%q]`, ex)
		}
	}
	for _, got := range errs {
		found := false
		for _, exp := range expectedErr {
			if trimWS(got.Error()) == trimWS(exp) {
				found = true
			}
		}
		if !found {
			t.Errorf(`Unexpected Error = [%q]`, got.Error())
		}
	}
}

func TestStmt0(t *testing.T) {

	input := `{
  me(func: eq(Name, "Steven Spielberg"))  {
    Name
    date_of_birth:DOB
    cn as count(Siblings)
    brothers: Siblings {
    		Name
  			Friends {
  				Name
  			}
	}
  }
}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)

	doc, errs := p.ParseInput()
	//

	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), input) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(input))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	t.Log(doc.String())
	//

}

func TestStmt1(t *testing.T) {

	input := `{
  me(func: gt(count(Director.film), 5)) { 
    totalDirectors : count(uid)
	}
	}`

	var parseErrs []string

	p := New(input)

	doc, errs := p.ParseInput()
	//

	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), input) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(input))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	t.Log(doc.String())
	//

}

func TestStmt2(t *testing.T) {

	input := `{
  me(func: gt(count(Director.film), 5)) { 
    totalDirectors : count(uid)
} }`

	expectedDoc := `{
 me(func: gt(count(Director.film), 5)) { 
    totalDirectors : count(uid)
} }`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)

	doc, errs := p.ParseInput()
	//

	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), expectedDoc) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("%s^", strings.Repeat(" ", diffPos(doc.String(), expectedDoc)))
			t.Logf("Expected: [%s] \n", trimWS(expectedDoc))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	t.Log(doc.String())
	//

}

func TestStmt3(t *testing.T) {

	input := `{
  meow(func: allofterms(Name, "eat drink man woman") ,first : 5) {
    Film.performance {
      actors as performance.actor {
         Name
      	totalRoles as count(Director.film)
      }
    }
  }
}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)

	doc, errs := p.ParseInput()
	//
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), input) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("%s^", strings.Repeat(" ", diffPos(doc.String(), input)+11))
			t.Logf("Expected: [%s] \n", trimWS(input))
			t.Errorf(`Unexpected document for %s. `, t.Name())
			t.Fail()
		}
	}

}

func TestStmt3a(t *testing.T) {

	input := `{
  meow(func: allofterms(Name, "eat drink man woman") ,first : 5.6) {
    Film.performance {
      actors as performance.actor {
         Name
      	totalRoles as count(Director.film)
      }
    }
  }
}`

	parseErrs := []string{"Expected an integer got 5.6 at line: 2, column: 63"}

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)

	doc, errs := p.ParseInput()
	//
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), input) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("%s^", strings.Repeat(" ", diffPos(doc.String(), input)+11))
			t.Logf("Expected: [%s] \n", trimWS(input))
			t.Errorf(`Unexpected document for %s. `, t.Name())
			t.Fail()
		}
	}
}

func TestStmt3b(t *testing.T) {

	input := `{
  meow(func: allofterms(Name, "eat drink man woman") ,first : 5) @filter(eq(Name,"Ross Payne")) {
    Film.performance {
      actors as performance.actor {
         Name
      	totalRoles as count(Director.film)
      }
    }
  }
}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)

	doc, errs := p.ParseInput()
	//
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), input) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("%s^", strings.Repeat(" ", diffPos(doc.String(), input)+11))
			t.Logf("Expected: [%s] \n", trimWS(input))
			t.Errorf(`Unexpected document for %s. `, t.Name())
			t.Fail()
		}
	}
	t.Log(doc.String())
}

func TestStmt3c(t *testing.T) {

	input := `{
  meow(func: allofterms(Name, "eat drink man woman") ,first : 5) @filter(eq(NameX,"Ross Payne")) {
    Film.performance {
      actors as performance.actor {
         Name
      	totalRoles as count(Director.film)
      }
    }
  }
}`

	parseErrs := []string{`"NameX" is not a predicate (scalar or uid-pred) in any known type at line: 2, column: 73`}

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)

	doc, errs := p.ParseInput()
	//
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), input) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("%s^", strings.Repeat(" ", diffPos(doc.String(), input)+11))
			t.Logf("Expected: [%s] \n", trimWS(input))
			t.Errorf(`Unexpected document for %s. `, t.Name())
			t.Fail()
		}
	}
	t.Log(doc.String())
}

func TestStmt3d(t *testing.T) {
	input := `{
	  me(func: eq(Name, "Steven Spielberg")) @filter(has(Director.film)) {
	    Name
	    Director.film @filter(allofterms(Name, "jones indiana") OR allofterms(Name, "jurassic park"))  {
	      Name
	    }
	  }
	}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)

	doc, errs := p.ParseInput()
	//
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), input) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("%s^", strings.Repeat(" ", diffPos(doc.String(), input)+11))
			t.Logf("Expected: [%s] \n", trimWS(input))
			t.Errorf(`Unexpected document for %s. `, t.Name())
			t.Fail()
		}
	}
	t.Log(doc.String())
}

func TestFilter0(t *testing.T) {

	input := `{
  me(func: eq(Name, "Steven Spielberg")) @filter(has(Friends)) {
   Name
}}`

	expectedDoc := `{
  me(func: eq(Name, "Steven Spielberg")) @filter(has(Friends)) {
   Name
}}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)

	doc, errs := p.ParseInput()
	//

	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), expectedDoc) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(expectedDoc))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	t.Log(doc.String())
	//

}

func TestCountPred(t *testing.T) {

	input := `{
  me(func: eq(count(name), "Steven Spielberg"))  {
}`

	expectedDoc := `{
  me(func: eq(count(name), "Steven Spielberg"))   {
}`

	parseErrs := []string{`"name" must be a uid-predicate at line: 2, column: 21`}

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)
	doc, errs := p.ParseDocument()
	t.Log(doc.String())
	//
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), expectedDoc) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(expectedDoc))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	//

}

func TestStmtInvalidPred(t *testing.T) {

	input := `{
  me(func: eq(Age, 67 ) ) {
  Name
  Jobs3
}}`

	expectedDoc := `{
 me(func: eq(Age, 67 )) {
   Name
  Jobs
}}`
	parseErrs := []string{`"Jobs3" is not a scalar-pred at line: 4, column: 3`}

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)
	doc, errs := p.ParseDocument()
	t.Log(doc.String())
	//
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), expectedDoc) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(expectedDoc))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	//

}

func TestStmtFloatPredErr(t *testing.T) {

	input := `{
  me(func: eq(age, 6732234.23 ) ) {
  Name
}}`

	expectedDoc := `{
 me(func: eq(age, 6.73223423E+06 )) {
 Name
}}`

	parseErrs := []string{`Predicate "age" is not a scalar in any known type at line: 2, column: 15`}

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)
	doc, errs := p.ParseDocument()
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), expectedDoc) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(expectedDoc))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	//

}

func TestStmtFloatPred(t *testing.T) {

	input := `{
  me(func: eq(Age, 6732234.23 ) ) {
}`

	expectedDoc := `{
 me(func: eq(Age, 6.73223423E+06 )) {
}}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)
	doc, errs := p.ParseDocument()
	t.Log(doc.String())
	//
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), expectedDoc) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(expectedDoc))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	//

}

func TestFilter1(t *testing.T) {

	input := `{
  me(func:eq(count(Siblings),2), first:2) @filter(has(Friends))  {
	Name
}}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)
	doc, errs := p.ParseDocument()
	//
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), input) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(input))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	t.Log(doc.String())
	//

}

func TestFilterAnd(t *testing.T) {

	input := `{
  me(func:eq(count(Siblings),2)) @filter(has(Friends) and (has(Siblings) or has(Name))) {
	Name
}}`

	var parseErrs []string

	// l := lexer.New(input)
	// p := New(l)
	p := New(input)
	doc, errs := p.ParseDocument()
	t.Log(doc.String())
	//
	checkErrors(errs, parseErrs, t)
	if len(errs) == 0 {
		if compare(doc.String(), input) {
			t.Logf("Got:      [%s] \n", trimWS(doc.String()))
			t.Logf("Expected: [%s] \n", trimWS(input))
			t.Errorf(`Unexpected document for %s. `, t.Name())
		}
	}
	//

}
