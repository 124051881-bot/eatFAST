const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const cors = require('cors');
const path = require('path');

// Cargar variables de entorno
require('dotenv').config({ path: path.resolve(__dirname, '.env') });

const app = express();
const server = http.createServer(app);

// Configuración de WebSockets
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST', 'PUT', 'DELETE']
    }
});

app.use(cors());
app.use(express.json());

process.on('uncaughtException', (err) => console.error('💥 [UNCAUGHT EXCEPTION]:', err));
process.on('unhandledRejection', (reason) => console.error('💥 [UNHANDLED REJECTION]:', reason));

// Helper para obtener credenciales de Railway o entorno local
const defaultDbConfig = {
    host: process.env.MYSQLHOST || process.env.DB_NORTE_HOST || 'localhost',
    user: process.env.MYSQLUSER || process.env.DB_NORTE_USER || 'root',
    password: process.env.MYSQLPASSWORD || process.env.DB_NORTE_PASSWORD || '',
    database: process.env.MYSQLDATABASE || process.env.DB_NORTE_NAME || 'railway',
    port: parseInt(process.env.MYSQLPORT || process.env.DB_NORTE_PORT || '3306', 10)
};

const dbConfigs = {
    1: { 
        host: process.env.DB_NORTE_HOST || defaultDbConfig.host, 
        user: process.env.DB_NORTE_USER || defaultDbConfig.user, 
        password: process.env.DB_NORTE_PASSWORD || defaultDbConfig.password, 
        database: process.env.DB_NORTE_NAME || defaultDbConfig.database,
        port: parseInt(process.env.DB_NORTE_PORT || defaultDbConfig.port, 10)
    },
    2: { 
        host: process.env.DB_SUR_HOST || defaultDbConfig.host, 
        user: process.env.DB_SUR_USER || defaultDbConfig.user, 
        password: process.env.DB_SUR_PASSWORD || defaultDbConfig.password, 
        database: process.env.DB_SUR_NAME || defaultDbConfig.database,
        port: parseInt(process.env.DB_SUR_PORT || defaultDbConfig.port, 10)
    },
    3: { 
        host: process.env.DB_CENTRO_HOST || defaultDbConfig.host, 
        user: process.env.DB_CENTRO_USER || defaultDbConfig.user, 
        password: process.env.DB_CENTRO_PASSWORD || defaultDbConfig.password, 
        database: process.env.DB_CENTRO_NAME || defaultDbConfig.database,
        port: parseInt(process.env.DB_CENTRO_PORT || defaultDbConfig.port, 10)
    }
};

// Generador de conexiones
async function getNodoConnection(id_region) {
    try {
        const regionValida = (!id_region || id_region === 0 || !dbConfigs[id_region]) ? 1 : id_region;
        const config = dbConfigs[regionValida];

        return await mysql.createConnection({
            host: config.host,
            user: config.user,
            password: config.password,
            database: config.database,
            port: config.port,
            connectTimeout: 10000
        });
    } catch (error) {
        console.error(`❌ [ERROR DB REGION ${id_region}]: ${error.message}`);
        throw new Error(`No se pudo conectar a la base de datos de la región ${id_region}: ${error.message}`);
    }
}

// WebSockets
io.on('connection', (socket) => {
    console.log(`⚡ [SOCKET CONECTADO]: ${socket.id}`);

    const unirseRegionHandler = (data) => {
        const id_region = typeof data === 'object' ? data.id_region : data;
        const roomName = `region_${id_region || 1}`;
        socket.join(roomName);
    };

    socket.on('join_region', unirseRegionHandler);
    socket.on('unirse_region', unirseRegionHandler);

    socket.on('escuchar_pedido', (id_pedido) => {
        socket.join(`pedido_${id_pedido}`);
    });

    socket.on('actualizar_ubicacion_repartidor', (data) => {
        const { id_pedido, id_region, lat, lng } = data;
        io.to(`region_${id_region || 1}`).emit('ubicacion_repartidor_actualizada', {
            id_pedido, lat, lng, timestamp: new Date().toISOString()
        });
    });

    socket.on('disconnect', () => console.log(`❌ [SOCKET DESCONECTADO]: ${socket.id}`));
});

// Rutas base
app.get('/', (req, res) => res.send("EatFast Backend Server is Running!"));
app.get('/api/health', (req, res) => res.json({ status: "online", system: "EatFast Engine v3.0" }));

// Autenticación
app.post('/api/auth/register', async (req, res) => {
    const { nombre, email, password, rol, id_region, idRegion } = req.body;
    const regionIdFinal = id_region !== undefined ? id_region : idRegion;
    const targetRegion = parseInt(regionIdFinal, 10);
    let connection;

    try {
        if (isNaN(targetRegion)) {
            return res.status(400).json({ success: false, message: "La región geográfica es requerida." });
        }

        if (!password || password.trim() === '') {
            return res.status(400).json({ success: false, message: "La contraseña es obligatoria." });
        }

        const nodoConexion = targetRegion === 0 ? 1 : targetRegion;
        connection = await getNodoConnection(nodoConexion);

        // Generar un id_usuario numérico único compatible con la columna INT de MySQL
        const id_usuario = Math.floor(Date.now() / 1000); 

        const query = `
            INSERT INTO usuarios (id_usuario, id_region, nombre, email, password, rol) 
            VALUES (?, ?, ?, ?, ?, ?)
        `;
        
        await connection.execute(query, [
            id_usuario, 
            nodoConexion, 
            nombre, 
            email.trim(), 
            password, 
            rol || 'cliente'
        ]);

        res.status(201).json({ 
            success: true, 
            message: "Registro exitoso.",
            usuario: { id_usuario, id_region: nodoConexion, nombre, email, rol: rol || 'cliente' }
        });

    } catch (error) {
        console.error(`[ERROR REGISTRO]: ${error.message}`);
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

app.post('/api/auth/login', async (req, res) => {
    const { email, password } = req.body;
    const regionInput = req.body.id_region !== undefined ? req.body.id_region : req.body.idRegion;
    const id_region = parseInt(regionInput, 10);
    let connection;

    try {
        const nodoAConectar = isNaN(id_region) || id_region === 0 ? 1 : id_region;
        connection = await getNodoConnection(nodoAConectar);
        
        const [rows] = await connection.execute(
            `SELECT id_usuario, id_region, nombre, email, password, rol FROM usuarios WHERE email = ?`,
            [email.trim()]
        );

        if (rows.length === 0) {
            return res.status(401).json({ success: false, message: "El usuario no existe." });
        }

        const usuario = rows[0];

        if (usuario.password !== password) {
            return res.status(401).json({ success: false, message: "Contraseña incorrecta." });
        }

        return res.status(200).json({
            success: true,
            user: {
                id_usuario: usuario.id_usuario,
                id_region: usuario.id_region,
                nombre: usuario.nombre,
                email: usuario.email,
                rol: usuario.rol
            }
        });

    } catch (error) {
        console.error(`[ERROR LOGIN]: ${error.message}`);
        return res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

// Pedidos
app.post('/api/pedidos/crear', async (req, res) => {
    const { id_usuario, id_region, id_restaurante, total, productos } = req.body;
    let connection;

    try {
        const usuarioTarget = id_usuario ?? null;
        const nodoTarget = parseInt(id_region, 10) || 1;
        const restauranteTarget = id_restaurante ? String(id_restaurante) : '1';
        const totalTarget = total !== undefined ? parseFloat(total) : 0.0;

        if (!usuarioTarget) {
            return res.status(400).json({ success: false, message: "El campo 'id_usuario' es obligatorio." });
        }

        connection = await getNodoConnection(nodoTarget);
        await connection.beginTransaction();

        const queryPedido = `
            INSERT INTO pedidos (id_usuario, id_region, id_restaurante, total, estado) 
            VALUES (?, ?, ?, ?, 'pendiente')
        `;
        
        const [resultPedido] = await connection.execute(queryPedido, [
            usuarioTarget, nodoTarget, restauranteTarget, totalTarget
        ]);

        const id_pedido = resultPedido.insertId;

        if (productos && Array.isArray(productos) && productos.length > 0) {
            const queryDetalle = `
                INSERT INTO pedido_detalles (id_pedido, id_producto, cantidad, precio_unitario) 
                VALUES (?, ?, ?, ?)
            `;
            for (const prod of productos) {
                await connection.execute(queryDetalle, [
                    id_pedido, prod.id_producto ?? null, prod.cantidad ?? 1, prod.precio_unitario ?? 0.0
                ]);
            }
        }

        await connection.commit();

        const nuevoPedido = {
            id_pedido, id_usuario: usuarioTarget, id_region: nodoTarget,
            id_restaurante: restauranteTarget, total: totalTarget, estado: 'pendiente'
        };

        io.to(`region_${nodoTarget}`).emit('nuevo_pedido', nuevoPedido);

        res.status(201).json({ success: true, message: "Pedido registrado exitosamente.", id_pedido, estado: 'pendiente' });

    } catch (error) {
        if (connection) await connection.rollback();
        console.error(`[ERROR CREAR PEDIDO]: ${error.message}`);
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

app.get('/api/pedidos/activos/:id_region', async (req, res) => {
    const { id_region } = req.params;
    let connection;

    try {
        const nodoTarget = parseInt(id_region, 10) || 1;
        connection = await getNodoConnection(nodoTarget);

        const [rows] = await connection.execute(
            `SELECT id_pedido, id_usuario, id_region, id_restaurante, total, estado, id_repartidor 
             FROM pedidos 
             WHERE estado IN ('pendiente', 'aceptado', 'cocinando', 'en camino', 'en_camino')
             ORDER BY id_pedido DESC`
        );

        res.status(200).json({ success: true, pedidos: rows });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

app.put('/api/pedidos/actualizar-estado', async (req, res) => {
    const { id_pedido, id_region, estado, nuevoEstado, id_repartidor } = req.body;
    const estadoFinal = estado || nuevoEstado;
    let connection;

    try {
        const nodoTarget = parseInt(id_region, 10) || 1;
        connection = await getNodoConnection(nodoTarget);

        const query = `
            UPDATE pedidos 
            SET estado = ?, id_repartidor = COALESCE(?, id_repartidor) 
            WHERE id_pedido = ?
        `;
        await connection.execute(query, [estadoFinal, id_repartidor || null, id_pedido]);

        const datosActualizados = { id_pedido, estado: estadoFinal, id_repartidor };
        io.to(`pedido_${id_pedido}`).emit('cambio_estado_pedido', datosActualizados);

        res.status(200).json({ success: true, message: `Estado actualizado a '${estadoFinal}'.`, estado: estadoFinal });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

// Productos
app.get('/api/productos/:id_region', async (req, res) => {
    const { id_region } = req.params;
    let connection;

    try {
        connection = await getNodoConnection(parseInt(id_region, 10) || 1);
        const [rows] = await connection.execute(`SELECT * FROM productos ORDER BY id_producto DESC`);
        res.status(200).json({ success: true, productos: rows });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`🚀 EatFast Engine corriendo en el puerto: ${PORT}`);
});