@echo off
:: ============================================
:: Actualiza el proyecto y ejecuta el script
:: ============================================

:: 📂 Ruta base del proyecto
cd /d "D:\API\cpd"

:: ⚙️ Actualizar código desde Git
echo 🔄 Actualizando repositorio...
git fetch --all
git reset --hard origin/master
echo ✅ Repositorio actualizado correctamente.

:: ============================================
:: Ejecutar el script con una ruta pasada como argumento
:: ============================================

:: 🧠 Ejemplo: pasar la ruta como parámetro al .bat
:: Uso: run_project.bat "D:\Datos\entrada\"
set ARG_PATH=%~1

if "%ARG_PATH%"=="" (
    echo ⚠️ No se proporcionó una ruta. Usa:
    echo     run_project.bat "D:\Datos\entrada\"
    pause
    exit /b
)

echo 🚀 Ejecutando main.py con ruta: %ARG_PATH%
start "" /B python main.py "%ARG_PATH%"
exit