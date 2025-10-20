@echo off
:: ============================================
:: Update the project and run the Python script
:: ============================================

:: 🧭 Move to the directory where this script is located
cd /d "%~dp0"

echo 🔄 Fetching latest changes from Git...
git fetch --all
git reset --hard origin/master
echo ✅ Repository successfully updated.

:: ============================================
:: Run the Python script and close the terminal
:: ============================================

echo 🚀 Running main.py...
start "" /B python main.py
exit