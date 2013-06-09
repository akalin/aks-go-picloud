package main

import "encoding/json"
import "github.com/akalin/aks-go/aks"
import "flag"
import "fmt"
import "log"
import "math/big"
import "os"
import "runtime"

type jsonResults struct {
	N       string `json:"n"`
	R       string `json:"r"`
	M       string `json:"M"`
	Start   string `json:"start"`
	End     string `json:"end"`
	Factor  string `json:"factor,omitempty"`
	Witness string `json:"witness,omitempty"`
	IsPrime bool   `json:"isPrime,omitempty"`
}

func getAKSWitness(n, r, M, start, end big.Int, jobs int) jsonResults {
	var results jsonResults
	results.N = n.String()
	results.R = r.String()
	results.M = M.String()
	results.Start = start.String()
	results.End = end.String()

	factor := aks.GetFirstFactorBelow(&n, &M)
	if factor != nil {
		results.Factor = factor.String()
		return results
	}

	// M^2 > N iff M > floor(sqrt(N)).
	var mSq big.Int
	mSq.Mul(&M, &M)
	if mSq.Cmp(&n) > 0 {
		results.IsPrime = true
		return results
	}

	logger := log.New(os.Stderr, "", 0)
	a := aks.GetAKSWitness(&n, &r, &start, &end, jobs, logger)
	if a != nil {
		results.Witness = a.String()
	} else if start.Cmp(big.NewInt(1)) <= 0 && end.Cmp(&M) >= 0 {
		results.IsPrime = true
	}
	return results
}

func main() {
	jobs := flag.Int(
		"j", runtime.NumCPU(), "how many processing jobs to spawn")
	startStr := flag.String(
		"start", "", "the lower bound to use (defaults to 1)")
	endStr := flag.String(
		"end", "", "the upper bound to use (defaults to M)")

	flag.Parse()

	runtime.GOMAXPROCS(*jobs)

	if flag.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "%s [options] [number]\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(-1)
	}

	var start big.Int
	if len(*startStr) > 0 {
		_, parsed := start.SetString(*startStr, 10)
		if !parsed {
			fmt.Fprintf(
				os.Stderr, "could not parse %s\n", *startStr)
			os.Exit(-1)
		}
	}

	var end big.Int
	if len(*endStr) > 0 {
		_, parsed := end.SetString(*endStr, 10)
		if !parsed {
			fmt.Fprintf(os.Stderr, "could not parse %s\n", *endStr)
			os.Exit(-1)
		}
	}

	var n big.Int
	_, parsed := n.SetString(flag.Arg(0), 10)
	if !parsed {
		fmt.Fprintf(os.Stderr, "could not parse %s\n", flag.Arg(0))
		os.Exit(-1)
	}

	one := big.NewInt(1)
	two := big.NewInt(2)

	if n.Cmp(two) < 0 {
		fmt.Fprintf(os.Stderr, "n must be >= 2\n")
		os.Exit(-1)
	}

	r := aks.CalculateAKSModulus(&n)
	M := aks.CalculateAKSUpperBound(&n, r)

	if start.Cmp(one) < 0 {
		start.Set(one)
	}
	if end.Sign() <= 0 {
		end.Set(M)
	}

	results := getAKSWitness(n, *r, *M, start, end, *jobs)
	resultsStr, err := json.Marshal(results)
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error())
		os.Exit(-1)
	}
	os.Stdout.Write(resultsStr)
}
