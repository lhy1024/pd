for i in {1..120}; do
    echo "start test"
    go clean -testcache
    cd ~/pd/tests/mcs/tso
    go test -run ^TestTSOServerTestSuite$ github.com/tikv/pd/tests/mcs/tso
    go test -run ^TestTSOPath$ github.com/tikv/pd/tests/mcs/tso
    go test -run ^TestAPIServerForwardTestSuite$ github.com/tikv/pd/tests/mcs/tso
    go test -run ^TestTSOServiceTestSuite$ github.com/tikv/pd/tests/mcs/tso
    cd ~/pd/tests/mcs/discovery
    go test -run ^TestServerRegisterTestSuite$ github.com/tikv/pd/tests/mcs/discovery
    cd ~/pd/tests/mcs/resource_manager
    go test -run ^TestResourceManagerServer$ github.com/tikv/pd/tests/mcs/resource_manager
done
for i in {1..120}; do
    echo "start test"
    go clean -testcache
    cd ~/pd/tests/mcs/tso
    go test -tags deadlock -race -run ^TestTSOServerTestSuite$ github.com/tikv/pd/tests/mcs/tso
    go test -tags deadlock -race -run ^TestTSOPath$ github.com/tikv/pd/tests/mcs/tso
    go test -tags deadlock -race -run ^TestAPIServerForwardTestSuite$ github.com/tikv/pd/tests/mcs/tso
    go test -tags deadlock -race -run ^TestTSOServiceTestSuite$ github.com/tikv/pd/tests/mcs/tso
    cd ~/pd/tests/mcs/discovery
    go test -tags deadlock -race -run ^TestServerRegisterTestSuite$ github.com/tikv/pd/tests/mcs/discovery
    cd ~/pd/tests/mcs/resource_manager
    go test -tags deadlock -race -run ^TestResourceManagerServer$ github.com/tikv/pd/tests/mcs/resource_manager
done
