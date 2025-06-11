for i in {1..200}
do
go clean -testcache
go test -timeout 120s -run ^TestOperatorControllerTestSuite$  github.com/tikv/pd/pkg/schedule/operator
done
