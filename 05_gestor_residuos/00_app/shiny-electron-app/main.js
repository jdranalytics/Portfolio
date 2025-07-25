const { app, BrowserWindow } = require('electron');
const path = require('path');
const child = require('child_process');
const net = require('net');
const ps = require('ps-node');

let mainWindow;
let loadingWindow;
let childProcess;
let currentPort;

// Función mejorada para encontrar puerto disponible
async function findAvailablePort(startPort) {
  let port = startPort;
  while (port < 65535) {
    if (await isPortAvailable(port)) {
      return port;
    }
    port++;
  }
  throw new Error('No se encontró puerto disponible');
}

// Verificación mejorada de puerto disponible
function isPortAvailable(port) {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.unref();
    server.on('error', () => resolve(false));
    server.listen({ port, host: '127.0.0.1' }, () => {
      server.close(() => resolve(true));
    });
  });
}

// Obtener rutas de archivos
function getAppPath() {
  return app.isPackaged
    ? path.join(process.resourcesPath, 'shiny-app', 'app.R')
    : path.join(__dirname, 'shiny-app', 'app.R');
}

function getRScriptPath() {
  if (process.platform !== 'win32') {
    throw new Error('Solo se soporta Windows');
  }
  return app.isPackaged
    ? path.join(process.resourcesPath, 'R-Portable', 'bin', 'RScript.exe')
    : path.join(__dirname, 'R-Portable', 'bin', 'RScript.exe');
}

// *** NUEVA FUNCIÓN PARA OBTENER LA RUTA DEL ICONO ***
function getIconPath() {
  // Cuando está empaquetado, el ícono estará en la carpeta 'resources'
  // Cuando no está empaquetado (en desarrollo), estará en la carpeta 'icons' junto a main.js
  return app.isPackaged
    ? path.join(process.resourcesPath, 'icons', 'JDR.ico')
    : path.join(__dirname, 'icons', 'JDR.ico');
}

// Esperar a que Shiny esté listo (versión mejorada)
async function waitForShinyReady(port, timeout = 45000) {
  const startTime = Date.now();
  
  return new Promise((resolve, reject) => {
    const check = () => {
      if (Date.now() - startTime > timeout) {
        reject(new Error(`Timeout esperando a Shiny (${timeout}ms)`));
        return;
      }

      const client = net.createConnection({ port, host: '127.0.0.1' }, () => {
        client.end();
        resolve();
      });

      client.on('error', (err) => {
        client.destroy();
        setTimeout(check, 1000);
      });
    };
    
    check();
  });
}

// Matar procesos en un puerto (versión mejorada para Windows)
function killProcessOnPort(port) {
  return new Promise((resolve) => {
    if (process.platform !== 'win32') {
      return resolve();
    }

    child.exec(`netstat -ano | findstr :${port}`, (err, stdout) => {
      if (err || !stdout) return resolve();

      const pids = new Set();
      stdout.trim().split('\n').forEach(line => {
        const parts = line.trim().split(/\s+/);
        if (parts.length >= 5) {
          pids.add(parts[4]);
        }
      });

      pids.forEach(pid => {
        try {
          process.kill(pid, 'SIGTERM');
          console.log(`Terminado proceso PID: ${pid}`);
        } catch (e) {
          console.error(`Error terminando proceso ${pid}:`, e.message);
        }
      });

      resolve();
    });
  });
}

// Limpieza mejorada
async function cleanupApp() {
  console.log('Iniciando limpieza...');
  
  // 1. Cerrar ventanas
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.close();
  }
  if (loadingWindow && !loadingWindow.isDestroyed()) {
    loadingWindow.close();
  }

  // 2. Terminar proceso R
  if (childProcess && !childProcess.killed) {
    console.log('Terminando proceso R...');
    try {
      childProcess.kill('SIGTERM');
      await new Promise(resolve => setTimeout(resolve, 2000));
      if (!childProcess.killed) {
        childProcess.kill('SIGKILL');
      }
    } catch (e) {
      console.error('Error terminando proceso R:', e.message);
    }
  }

  // 3. Liberar puerto si es necesario
  if (currentPort) {
    console.log(`Liberando puerto ${currentPort}...`);
    await killProcessOnPort(currentPort);
  }

  console.log('Limpieza completada.');
}

// Función principal mejorada
async function createMainWindow() {
  try {
    currentPort = await findAvailablePort(8080);
    console.log(`Puerto seleccionado: ${currentPort}`);

    const iconPath = getIconPath(); // *** OBTENEMOS LA RUTA DEL ICONO ***

    // Mostrar ventana de carga
    loadingWindow = new BrowserWindow({
      width: 400,
      height: 200,
      frame: false,
      show: false,
      webPreferences: { nodeIntegration: true },
      icon: iconPath // *** ASIGNAMOS EL ICONO A LA VENTANA DE CARGA ***
    });
    loadingWindow.loadFile('loading.html');
    loadingWindow.show();

    // Iniciar Shiny
    const rScriptPath = getRScriptPath();
    const appPath = getAppPath();
    const shinyCmd = `"${rScriptPath}" --encoding=UTF-8 -e "shiny::runApp('${appPath.replace(/\\/g, '\\\\')}', port=${currentPort}, host='127.0.0.1')"`;
    
    console.log('Iniciando Shiny:', shinyCmd);
    childProcess = child.exec(shinyCmd, { windowsHide: true });

    // Manejar salida de Shiny
    childProcess.stdout.on('data', (data) => {
      console.log(`Shiny: ${data}`);
      if (data.includes('Listening on')) {
        console.log('Shiny está listo!');
      }
    });

    childProcess.stderr.on('data', (data) => {
      console.error(`Shiny error: ${data}`);
    });

    childProcess.on('close', (code) => {
      console.log(`Proceso Shiny terminado con código ${code}`);
      if (code !== 0 && !mainWindow.isDestroyed()) {
        mainWindow.loadURL(`data:text/html,<h1>Error</h1><p>Shiny se cerró con código ${code}</p>`);
      }
    });

    // Esperar a que Shiny esté listo
    console.log('Esperando a que Shiny esté listo...');
    await waitForShinyReady(currentPort);

    // Crear ventana principal
    mainWindow = new BrowserWindow({
      width: 1200,
      height: 800,
      show: false,
      webPreferences: {
        nodeIntegration: false,
        contextIsolation: true
      },
      icon: iconPath // *** ASIGNAMOS EL ICONO A LA VENTANA PRINCIPAL ***
    });

    mainWindow.loadURL(`http://127.0.0.1:${currentPort}`);
    
    mainWindow.webContents.on('did-finish-load', () => {
      console.log('Aplicación cargada, mostrando ventana principal');
      mainWindow.show();
      if (loadingWindow && !loadingWindow.isDestroyed()) {
        loadingWindow.close();
      }
    });

    mainWindow.on('closed', () => {
      mainWindow = null;
    });

  } catch (error) {
    console.error('Error durante la inicialización:', error);
    
    // Mostrar error en la ventana de carga
    if (loadingWindow && !loadingWindow.isDestroyed()) {
      loadingWindow.loadURL(`data:text/html,<h1>Error</h1><p>${error.message}</p>`);
    }
    
    await cleanupApp();
  }
}

// Manejo de eventos de la aplicación
app.whenReady().then(createMainWindow);

app.on('window-all-closed', async () => {
  if (process.platform !== 'darwin') {
    await cleanupApp();
    app.quit();
  }
});

app.on('activate', () => {
  if (mainWindow === null) {
    createMainWindow();
  }
});

// Manejar señales de terminación
process.on('SIGINT', async () => {
  await cleanupApp();
  process.exit(0);
});

process.on('SIGTERM', async () => {
  await cleanupApp();
  process.exit(0);
});