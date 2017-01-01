GOPATH=/Users/mac/Go/src/github.com/user/6.824/
export GOPATH
for ((i=10; i < 30; i++))
do
    go test > $i.txt
done