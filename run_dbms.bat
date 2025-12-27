@echo off
title Mini-Redis/Cassandra Control Panel
cls
:MENU
cls
echo ================================================================
echo    Mini-Redis/Cassandra Distributed Database - Control Panel
echo ================================================================
echo.
echo  [1] Run Automated Showcase Demo
echo  [2] Start 3-Node Cluster (Manual)
echo  [3] Launch CLI Client
echo  [4] Run Project Validation
echo  [5] Quick Health Check
echo  [6] View Walkthrough
echo  [0] Exit
echo.
echo ================================================================
set /p choice="Select option: "

if "%choice%"=="1" goto DEMO
if "%choice%"=="2" goto CLUSTER
if "%choice%"=="3" goto CLI
if "%choice%"=="4" goto VALIDATE
if "%choice%"=="5" goto QUICK
if "%choice%"=="6" goto WALKTHROUGH
if "%choice%"=="0" goto EXIT
goto MENU

:DEMO
cls
echo ================================================================
echo Running Automated Showcase Demo...
echo ================================================================
echo.
python examples\failure_demo.py
pause
goto MENU

:CLUSTER
cls
echo ================================================================
echo Starting 3-Node Cluster
echo ================================================================
echo.
echo Starting 3 Nodes in separate windows...
echo.
echo Launching Node 1 (Leader Candidate)...
start "Mini-Redis Node 1" cmd /k "python -m minidb.main --node-id node1 --port 7001 --cluster-port 8001 --data-dir ./data/node1"
timeout /t 2 >nul
echo Launching Node 2...
start "Mini-Redis Node 2" cmd /k "python -m minidb.main --node-id node2 --port 7002 --cluster-port 8002 --data-dir ./data/node2 --seed localhost:8001"
timeout /t 2 >nul
echo Launching Node 3...
start "Mini-Redis Node 3" cmd /k "python -m minidb.main --node-id node3 --port 7003 --cluster-port 8003 --data-dir ./data/node3 --seed localhost:8001"
echo.
echo Cluster started! You can now use Option [3] to connect via CLI.
pause
goto MENU

:CLI
cls
echo ================================================================
echo Launching CLI Client
echo ================================================================
echo.
echo Connecting to localhost:7001...
echo Type 'HELP' for available commands
echo.
python -m minidb.cli localhost 7001
pause
goto MENU

:VALIDATE
cls
echo ================================================================
echo Running Project Validation
echo ================================================================
echo.
python tests\test_validation.py
pause
goto MENU

:QUICK
cls
echo ================================================================
echo Running Quick Health Check
echo ================================================================
echo.
python tests\test_quick.py
pause
goto MENU

:WALKTHROUGH
cls
echo ================================================================
echo Mini-Redis/Cassandra Walkthrough
echo ================================================================
echo.
echo 1. Start a 3-node cluster using Option [2]
echo 2. Connect via CLI using Option [3]
echo 3. Try these commands:
echo.
echo    SET user:1 "Alice"     - Store a value
echo    GET user:1             - Retrieve value
echo    NODES                  - View cluster nodes
echo    LEADER                 - Show current leader
echo    RING                   - View hash ring
echo    SHARDS                 - View data distribution
echo    CONSISTENCY QUORUM     - Set consistency level
echo    HELP                   - Show all commands
echo.
echo 4. Kill a node (Ctrl+C) and watch election happen
echo 5. Use FAILOVER to force a new leader election
echo.
pause
goto MENU

:EXIT
exit
