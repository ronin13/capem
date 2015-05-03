package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	cstr  = "gcomm://Dock1"
	targz = "http://jenkins.percona.com/job/build-xtradb-cluster-binaries-56/BUILD_TYPE=release,label_exp=centos7-64/lastSuccessfulBuild/artifact/target/Percona-XtraDB-Cluster-5.6.22-25.8.262d88d73b6da1230a969e6148a59aeb408e7107.Linux.x86_64.tar.gz"
	MAXC  = 20
)

var containerMap map[string]string
var cmdsRun chan *container
var upNodes chan *container

var platform string
var addop string
var dockerImage string
var dnsIp string
var runC string
var cmd string
var ecmd string
var nodecnt int
var numc int
var rcount int
var jobDir string
var spath string
var autoinc string
var numthread int
var osize uint
var ocount uint
var hostsFile *os.File
var reap []*exec.Cmd

type container struct {
	name      string
	ipaddr    string
	pid       int
	segment   uint
	host      string
	port      int
	socket    string
	bootstrap bool
}

var conNodes []*container

func inspectContainer(con string, field string) (val string) {
	str := runWithMsg(fmt.Sprintf("docker inspect -f '%s' %s", field, con), fmt.Sprintf("Failed to inspect container %s for %s", con, field))
	return strings.Replace(str, "\n", "", -1)
}

func appendToFile(file string, data string) error {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatalf("%s", err)
	}

	_, err = f.WriteString(data)
	if err != nil {
		log.Fatalf("Write %s", err)
	}

	f.Close()

	return err
}

func runContainer(cName string, segment uint, bootstrap bool) {
	var cont *container
	cmdline := fmt.Sprintf("docker run %s", containerMap[cName])
	log.Printf("Starting container %s with %s", cName, cmdline)

	_ = runWithMsg(cmdline, fmt.Sprintf("Container %s failed to start", cName))

	//time.Sleep(time.Second * 60)
	ipaddr := inspectContainer(cName, "{{.NetworkSettings.IPAddress}}")

	if cName == "dnscluster" {
		dnsIp = ipaddr
		cont = &container{cName, ipaddr, 0, segment, "", 0, "", bootstrap}
	} else {
		data := fmt.Sprintf("%s %s\n%s %s.ci.percona.com\n", ipaddr, cName, ipaddr, cName)
		if err := appendToFile(hostsFile.Name(), data); err != nil {
			log.Fatalf("Failed to write ip address to hosts file")
		}
		pid, _ := strconv.Atoi(inspectContainer(cName, "{{.State.Pid}}"))
		port, _ := strconv.Atoi(inspectContainer(cName, "{{(index (index .NetworkSettings.Ports \"3306/tcp\") 0).HostPort}}"))
		host := inspectContainer(cName, "{{(index (index .NetworkSettings.Ports \"3306/tcp\") 0).HostIp}}")
		cont = &container{cName, ipaddr, pid, segment, host, port, "", bootstrap}
		waitForNode(cont)
		conNodes = append(conNodes, cont)
	}

	// To wait in the end
	cmdsRun <- cont

}

func runWithMsg(cmd string, msg string) string {
	var rval []byte
	var err error
	log.Printf("Running %s", cmd)

	if rval, err = exec.Command("sh", "-c", cmd).Output(); err != nil {
		if len(msg) > 0 {
			log.Fatalf(msg)
		}
	}
	return string(rval)
}

func backrunWithMsg(cmd string, msg string) *exec.Cmd {
	log.Printf("Running %s in background", cmd)

	cmnd := exec.Command("sh", "-c", cmd)

	if err := cmnd.Start(); err != nil {
		if len(msg) > 0 {
			log.Fatalf(msg)
		}
	}
	return cmnd
}

func killandWait() {
	close(cmdsRun)
	for cname := range cmdsRun {
		log.Printf("Stopping container %s", cname.name)
		_ = runWithMsg(fmt.Sprintf("docker stop %s", cname.name), fmt.Sprintf("Container %s failed to stop", cname.name))

	}

	for _, r := range reap {
		r.Process.Kill()
		r.Wait()
	}
}

func parseArgs() {
	flag.StringVar(&platform, "platform", "centos7-64", "The platform to build with")
	flag.StringVar(&addop, "addop", "evs.auto_evict=3; evs.version=1; gcache.size=256M", "Additional options")
	flag.StringVar(&cmd, "cmd", "/pxc/bin/mysqld --defaults-extra-file=/etc/my.cnf --basedir=/pxc --user=mysql --skip-grant-tables --query_cache_type=0  --wsrep_slave_threads=16 --innodb_autoinc_lock_mode=2  --query_cache_size=0 --innodb_flush_log_at_trx_commit=0 --innodb_file_per_table ", "Command to run")
	flag.StringVar(&ecmd, "ecmd", "--wsrep-sst-method=rsync --core-file ", "Additional command")
	flag.IntVar(&numc, "numc", 3, "Number of containers")
	flag.IntVar(&rcount, "rcount", 10, "Number of retries")
	flag.StringVar(&spath, "spath", "/pxc56/db", "Sysbench lua db")
	flag.StringVar(&autoinc, "autoinc", "off", "Auto-inc for sysbench")
	flag.IntVar(&numthread, "numt", 8, "Number of sysbench threads")
	flag.UintVar(&osize, "osize", 500, "Size of each table")
	flag.UintVar(&ocount, "ocount", 8, "Number of tables")
}

func buildImage() {
	log.Printf("Building %s from %s", dockerImage, targz)
	tarfile := "Percona-XtraDB-Cluster.tar.gz"

	dir, _ := os.Getwd()

	os.Chdir("docker")

	if _, err := os.Stat(tarfile); os.IsNotExist(err) {
		_ = runWithMsg(fmt.Sprintf("wget -O %s %s", tarfile, targz), fmt.Sprintf("Failed to download tar.gz from %s", targz))
	}

	_ = runWithMsg(fmt.Sprintf("docker build --rm -t %s -f Dockerfile.%s .", dockerImage, platform), fmt.Sprintf("Failed to build image for %s", dockerImage))

	os.Chdir(dir)

}

func loadData() {
	node := <-upNodes
	cmd := "sysbench --test=%s/parallel_prepare.lua ---report-interval=10  --oltp-auto-inc=%s --mysql-db=test  --db-driver=mysql --num-threads=%d --mysql-engine-trx=yes --mysql-table-engine=innodb --mysql-socket=%s --mysql-user=root  --oltp-table-size=%d --oltp_tables_count=%d prepare"

	cmd = fmt.Sprintf(cmd, spath, autoinc, numthread, node.socket, osize, ocount)

	runWithMsg(cmd, "Failed to run sysbench to load data")
}

func startNode(bootstrap bool) {
	var cmnd string
	var nodeName string = "Dock" + fmt.Sprintf("%d", nodecnt)

	if bootstrap {
		cmnd = fmt.Sprintf(runC, nodecnt, nodecnt, nodecnt, " --wsrep-new-cluster")
	} else {
		cmnd = fmt.Sprintf(runC, nodecnt, nodecnt, nodecnt, " ")
	}
	containerMap[nodeName] = cmnd
	runContainer(nodeName, 0, bootstrap)
	nodecnt++

}

func startOthers() {
	for i := 2; i <= numc; i++ {
		startNode(false)
	}
}

func waitForNode(node *container) {
	time.Sleep(time.Second * 10)
	_ = runWithMsg(fmt.Sprintf("mysqladmin -h %s -wait=%d -P %d -u root ping &>/dev/null", node.host, rcount, node.port), fmt.Sprintf("Node %s failed to come up", node.name))
	//if node.bootstrap {
	//spawnSock(node)
	//} else {
	go spawnSock(node)
	//}
}

func spawnSock(node *container) {
	sock := jobDir + "/" + node.name + ".sock"

	cmnd := backrunWithMsg(fmt.Sprintf("socat UNIX-LISTEN:%s,fork,reuseaddr TCP:%s:%d", sock, node.host, node.port), fmt.Sprintf("Failed to spawn socket %s for %s", sock, node.name))
	//defer cmnd.Wait()
	reap = append(reap, cmnd)

	node.socket = sock
	upNodes <- node

}
func preClean() {

	_ = runWithMsg("docker rm -f dnscluster", "")
	for i := 0; i < MAXC; i++ {
		_ = runWithMsg("docker stop Dock"+fmt.Sprintf("%d", i), "")
		_ = runWithMsg("docker rm -f Dock"+fmt.Sprintf("%d", i), "")
	}
	_ = runWithMsg("pkill -9 -f socat", "")
	_ = runWithMsg("pkill -9 -f mysqld", "")
}

func main() {
	var err error
	parseArgs()
	flag.Parse()

	cmdsRun = make(chan *container, numc+1)
	upNodes = make(chan *container, numc+1)
	nodecnt = 1
	runtime.GOMAXPROCS(10)

	preClean()

	dockerImage = "ronin/pxc:tarball-" + platform
	buildImage()

	if jobDir, err = ioutil.TempDir("", "capem"); err != nil {
		log.Fatalf("Failed to created temporary directory %s", jobDir)
	}

	if hostsFile, err = ioutil.TempFile(jobDir, "dns"); err != nil {
		log.Fatalf("Failed to created tempfile")
	}
	defer os.Remove(hostsFile.Name())

	containerMap = map[string]string{
		"dnscluster": fmt.Sprintf(" -d  -i -v /dev/log:/dev/log -e SST_SYSLOG_TAG=dnsmasq -v %s:/dnsmasq.hosts --name dnscluster ronin/dnsmasq &>/tmp/dnscluster-run.log", hostsFile.Name()),
	}

	defer killandWait()
	runContainer("dnscluster", 0, false)
	runC = "-P -e SST_SYSLOG_TAG=Dock%d --name Dock%d  -d  -i -v /dev/log:/dev/log -h Dock%d " + fmt.Sprintf(" --dns %s %s bash -c \"ulimit -c unlimited && %s %s --wsrep-provider-options='%s'\" &>/dev/null", dnsIp, dockerImage, cmd, ecmd, addop) + " %s"

	// bootstrap
	startNode(true)
	loadData()

	startOthers()
	time.Sleep(time.Second * 100)

}
