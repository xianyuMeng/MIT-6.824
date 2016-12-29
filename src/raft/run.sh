max=100
GOPATH=/Users/mac/Go/src/github.com/user/6.824/
export GOPATH
for ((i=0; i < 10; i++))
do
    go test > $i.txt
done