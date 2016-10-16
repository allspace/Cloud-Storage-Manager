package util

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
)

var (
	sectionRegex = regexp.MustCompile(`^\[(.*)\]$`)
	assignRegex  = regexp.MustCompile(`^([^=]+)=(.*)$`)
)

// ErrSyntax is returned when there is a syntax error in an INI file.
type ErrCfg struct {
	Code   int
	Msg    string
	Line   int
	Source string // The contents of the erroneous line, without leading or trailing whitespace
}

func (e ErrCfg) Error() string {
	return fmt.Sprintf("invalid INI syntax on line %d: %s", e.Line, e.Source)
}

func NewErr(code int, msg string) *ErrCfg {
	return &ErrCfg{
		Code: code,
		Msg:  msg,
	}
}

type AppCfg struct {
	cfg map[string]string

	fileName string
}

var Default AppCfg

func (me *AppCfg) loadEnv() {
	var val string

	val = os.Getenv("CS_ENDPOINT")
	if len(val) > 0 {
		me.cfg["CS_ENDPOINT"] = os.Getenv("CS_ENDPOINT")
	}
	val = os.Getenv("CS_REGION")
	if len(val) > 0 {
		me.cfg["CS_REGION"] = os.Getenv("CS_REGION")
	}
	val = os.Getenv("CS_KEY_ID")
	if len(val) > 0 {
		me.cfg["CS_KEY_ID"] = os.Getenv("CS_KEY_ID")
	}
	val = os.Getenv("CS_KEY_DATA")
	if len(val) > 0 {
		me.cfg["CS_KEY_DATA"] = os.Getenv("CS_KEY_DATA")
	}
	val = os.Getenv("CS_BUCKET")
	if len(val) > 0 {
		me.cfg["CS_BUCKET"] = os.Getenv("CS_BUCKET")
	}
	val = os.Getenv("CS_VENDOR_TYPE")
	if len(val) > 0 {
		me.cfg["CS_VENDOR_TYPE"] = os.Getenv("CS_VENDOR_TYPE")
	}
}

func (me *AppCfg) loadCmdArgs() {
	n := len(os.Args)
	for i := 1; i < n; i++ { //skip command name itself
		arg := os.Args[i]
		if len(arg) < 5 || arg[0] != '-' || arg[1] != '-' {
			log.Printf("Unrecogonized command line argument %s.", arg)
			continue
		}
		arg = arg[2:]
		j := strings.IndexByte(arg, '=')
		if j == -1 {
			key := strings.Replace(arg, "-", "_", -1)
			me.cfg[strings.ToUpper(key)] = "1"

		} else {
			key := strings.Replace(arg[0:j], "-", "_", -1)
			me.cfg[strings.ToUpper(key)] = arg[j:]
		}
	}
}

func (me *AppCfg) loadCfgFile(fileName string) {
	in, err := os.Open(fileName)
	if err != nil {
		log.Println(err)
		return
	}
	defer in.Close()
	bufin := bufio.NewReader(in)
	me.parseFile(bufin)
}

func (me *AppCfg) parseFile(in *bufio.Reader) (err error) {
	lineNum := 0
	for done := false; !done; {
		var line string
		if line, err = in.ReadString('\n'); err != nil {
			if err == io.EOF {
				done = true
			} else {
				return
			}
		}
		lineNum++
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			// Skip blank lines
			continue
		}
		if line[0] == ';' || line[0] == '#' {
			// Skip comments
			continue
		}

		if groups := assignRegex.FindStringSubmatch(line); groups != nil {
			key, val := groups[1], groups[2]
			key, val = strings.TrimSpace(key), strings.TrimSpace(val)
			me.cfg[strings.ToUpper(key)] = val
			//log.Println(val)
		} /* else if groups := sectionRegex.FindStringSubmatch(line); groups != nil {
			name := strings.TrimSpace(groups[1])
			section = name
			// Create the section if it does not exist
			file.Section(section)
		} else {
			return ErrSyntax{lineNum, line}
		}*/

	}
	return nil
}

///////////////////////////////////////////////////////////////////////////////

func (me *AppCfg) Init(fileName string) {

	if me.cfg == nil {
		me.cfg = make(map[string]string)
	}

	if len(fileName) > 0 {
		me.fileName = fileName
	} else { //set default config file path
		dir, file := path.Split(os.Args[0])
		i := strings.LastIndexByte(file, '.')
		if i != -1 {
			file = file[0:i]
		}
		me.fileName = path.Join(dir, file+".cfg")
	}

	me.Reload()
}

func (me *AppCfg) Reload() {
	if len(me.fileName) > 0 {
		me.loadCfgFile(me.fileName)
	}
	me.loadEnv()
	me.loadCmdArgs()
}

func (me *AppCfg) GetString(key string) (string, error) {
	val, ok := me.cfg[strings.ToUpper(key)]
	if ok {
		return val, nil
	} else {
		return "", NewErr(-1, "Not exists.")
	}
}

func (me *AppCfg) GetStringEx(key string, dft string) string {
	val, err := me.GetString(key)
	if err == nil {
		return val
	} else {
		return dft
	}
}

func (me *AppCfg) GetInt(key string) (int, error) {
	val, ok := me.cfg[strings.ToUpper(key)]
	if ok {
		return strconv.Atoi(val)
	} else {
		return 0, NewErr(-1, "Not exists.")
	}
}

func (me *AppCfg) GetIntEx(key string, dft int) int {
	val, err := me.GetInt(key)
	if err == nil {
		return val
	} else {
		return dft
	}
}

func (me *AppCfg) GetBool(key string) (bool, error) {
	val, ok := me.cfg[strings.ToUpper(key)]
	if ok {
		if val == "1" || strings.ToLower(val) == "true" {
			return true, nil
		} else {
			return false, nil
		}
	}
	return false, NewErr(-1, "Not exists.")
}

func (me *AppCfg) PrintAll() {
	for key, val := range me.cfg {
		log.Printf("%s = %s", key, val)
	}
}
