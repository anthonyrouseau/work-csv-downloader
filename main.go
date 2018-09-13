package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

func main() {
	//base url for getting csvs
	urlBase := "https://1md.org/admin/analytics/reporting/export/report"
	// want to iterate over platform, range
	platforms := []string{"aws", "awd", "bad", "fba", "gnn", "gns", "ytb", "tab"}
	// mapping platforms for query to platform names for report
	platformsFull := map[string]string{
		"aws": "adwords",
		"awd": "display",
		"bad": "bing",
		"fba": "facebook",
		"gnn": "native",
		"gns": "yahoo",
		"ytb": "youtube",
		"tab": "taboola"}
	//platforms := []string{"aws"}
	firstTouch := []string{"awd"}
	// want range dates in RFC3339 without the time
	now := time.Now()
	// mapping time frames for report to days ago from today
	timeFrames := map[string]TimeFrame{
		"2w": TimeFrame{start: -14, end: -1},
		"4w": TimeFrame{start: -28, end: -15},
		"lm": TimeFrame{start: -30, end: -1},
		"pm": TimeFrame{start: -60, end: -31}}
	var wg sync.WaitGroup                                       // waitgroup for csv fetches
	today := getDate(now)                                       // today in formate yyyy-mm-dd
	folder := strings.Join([]string{"./downloads", today}, "/") // name folder for today's csvs
	os.MkdirAll(folder, os.ModePerm)                            // create folder
	for _, platform := range platforms {                        // range over platforms and time frames and download csvs
		for frame := range timeFrames {
			var queryParams string
			r := getRange(now, timeFrames[frame])
			if Include(firstTouch, platform) {
				queryParams = getQueryParams(platform, r, "first")
			} else {
				queryParams = getQueryParams(platform, r, "last")
			}
			query := urlBase + "?" + queryParams
			wg.Add(1) // add another wait for the go routine starting next
			fileName := strings.Join([]string{today, platformsFull[platform], frame}, "_")
			go getCsv(query, strings.Join([]string{folder, fileName}, "/"), &wg) // each csv fetch in go routine to fetch in parallel
		}
	}
	wg.Wait() // wait for all csvs to come back
	fmt.Print("finished")
}

func getCsv(query string, path string, wg *sync.WaitGroup) error {
	defer wg.Done()                                               // let waitgroup know this go routine is done
	f, err := os.Create(strings.Join([]string{path, ".csv"}, "")) // create file to copy received data to
	if err != nil {
		return fmt.Errorf("Could not create file: %s", path)
	}
	defer f.Close()                                // need to close file when done
	client := &http.Client{}                       // making client to pass cookies with request, needed to access Scale
	req, err := http.NewRequest("GET", query, nil) // create request
	if err != nil {
		return fmt.Errorf("Could not create request for: %s", path)
	}
	sessId := os.Getenv("PHPSESSID")
	cookie := strings.Join([]string{"PHPSESSID=", sessId}, "")
	req.Header.Add("Cookie", cookie) // add the cookie using my session id from scale
	resp, err := client.Do(req)      // execute the request
	defer resp.Body.Close()          // close response body
	if err != nil {
		return fmt.Errorf("Could not get csv for file: %s", path)
	}
	_, err = io.Copy(f, resp.Body) // copy response into file created earlier
	if err != nil {
		return fmt.Errorf("Could not copy csv to: %s", path)
	}
	return nil
}

func getQueryParams(p, r, tp string) string {
	queryParams := url.Values{
		"group":                        []string{"wh_filters.campaign"},
		"report":                       []string{"acquisition"},
		"filters[wh_filters.platform]": []string{p},
		"filters[touchpoint]":          []string{tp},
		"filters[wh_devices.device]":   []string{},
		"range":    []string{r},
		"search":   []string{},
		"types[0]": []string{"1"},
		"types[1]": []string{"4"},
		"types[2]": []string{"7"},
		"types[3]": []string{"6"},
		"types[4]": []string{"5"}}
	return queryParams.Encode()

}

func getRange(ref time.Time, tf TimeFrame) string {
	start := ref.AddDate(0, 0, tf.start)
	end := ref.AddDate(0, 0, tf.end)
	startDate := getDate(start)
	endDate := getDate(end)
	return fmt.Sprintf("%s - %s", startDate, endDate)
}

func getDate(t time.Time) string {
	tFormatted := t.Format(time.RFC3339)
	tSplit := strings.Split(tFormatted, "T")
	return tSplit[0]
}

func Index(vs []string, t string) int {
	for i, v := range vs {
		if v == t {
			return i
		}
	}
	return -1
}

func Include(vs []string, t string) bool {
	return Index(vs, t) >= 0
}

type TimeFrame struct {
	start int
	end   int
}
