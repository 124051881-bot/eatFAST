const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const cors = require('cors');
const path = require('path');
const os = require('os');
const { v4: uuidv4 } = require('uuid');

// Cargar variables de entorno desde el .env local
require('dotenv').config({ path: path.resolve(__dirname, '.env') });

const app = express();
const server = http.createServer(app);

// Configuración de WebSockets con Socket.IO para tiempo real
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST', 'PUT', 'DELETE']
    }
});

app.use(cors());
app.use(express.json());

// Mapa de configuraciones regionales
const dbConfigs = {
    1: { 
        host: process.env.DB_NORTE_HOST, 
        user: process.env.DB_NORTE_USER, 
        password: process.env.DB_NORTE_PASSWORD, 
        database: process.env.DB_NORTE_NAME 
    },
    2: { 
        host: process.env.DB_SUR_HOST, 
        user: process.env.DB_SUR_USER, 
        password: process.env.DB_SUR_PASSWORD, 
        database: process.env.DB_SUR_NAME 
    },
    3: { 
        host: process.env.DB_CENTRO_HOST, 
        user: process.env.DB_CENTRO_USER, 
        password: process.env.DB_CENTRO_PASSWORD, 
        database: process.env.DB_CENTRO_NAME 
    }
};

// Generador dinámico de conexiones a bases de datos distribuidas
async function getNodoConnection(id_region) {
    const regionValida = id_region === 0 || !dbConfigs[id_region] ? 1 : id_region;
    const config = dbConfigs[regionValida];
    if (!config || !config.host) {
        throw new Error(`Configuración de red no encontrada para el Nodo Región ${id_region}.`);
    }
    return await mysql.createConnection(config);
}

// ====================================================================
// 🔌 SALAS EN TIEMPO REAL (SOCKET.IO)
// ====================================================================
io.on('connection', (socket) => {
    console.log(`⚡ [SOCKET CONECTADO]: ${socket.id}`);

    // Suscribir dispositivo a la sala de su región (ej. 'region_1')
    socket.on('unirse_region', (data) => {
        const id_region = typeof data === 'object' ? data.id_region : data;
        const roomName = `region_${id_region || 1}`;
        socket.join(roomName);
        console.log(`👥 Socket ${socket.id} unido a la sala: ${roomName}`);
    });

    // Suscribir dispositivo al monitoreo de un pedido específico
    socket.on('escuchar_pedido', (id_pedido) => {
        socket.join(`pedido_${id_pedido}`);
        console.log(`📦 Socket ${socket.id} escuchando cambios del Pedido #${id_pedido}`);
    });

    // Transmisión GPS en tiempo real
    socket.on('actualizar_ubicacion_repartidor', (data) => {
        const { id_pedido, id_region, lat, lng } = data;
        io.to(`region_${id_region || 1}`).emit('ubicacion_repartidor_actualizada', {
            id_pedido,
            lat,
            lng,
            timestamp: new Date().toISOString()
        });
    });

    socket.on('disconnect', () => {
        console.log(`❌ [SOCKET DESCONECTADO]: ${socket.id}`);
    });
});

// ====================================================================
// 💓 HEALTH CHECK
// ====================================================================
app.get('/api/health', (req, res) => {
    res.json({ status: "online", system: "EatFast Distributed Core Engine v3.0" });
});

// ====================================================================
// 🏢 AUTENTICACIÓN (LOGIN & REGISTRO)
// ====================================================================
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

        if (!dbConfigs[nodoConexion]) {
            return res.status(400).json({ success: false, message: "Región geográfica inválida." });
        }

        connection = await getNodoConnection(nodoConexion);
        const id_usuario = 'usr-' + uuidv4().substring(0, 8);

        const query = `
            INSERT INTO usuarios (id_usuario, id_region, nombre, email, password, rol) 
            VALUES (?, ?, ?, ?, ?, ?)
        `;
        
        await connection.execute(query, [
            id_usuario, 
            targetRegion, 
            nombre, 
            email.trim(), 
            password, 
            rol || 'cliente'
        ]);

        res.status(201).json({ 
            success: true, 
            message: "Registro exitoso.",
            usuario: { id_usuario, id_region: targetRegion, nombre, email, rol }
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
        if (isNaN(id_region)) {
            return res.status(400).json({ success: false, message: "ID de región inválido." });
        }

        const nodoAConectar = id_region === 0 ? 1 : id_region;
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

// ====================================================================
// 🛒 PEDIDOS (CRUD COMPLETO)
// ====================================================================

// 🟢 ALTA: Crear Pedido + Emitir Socket.IO al Repartidor (CORREGIDO)
app.post('/api/pedidos/crear', async (req, res) => {
    const { id_usuario, id_region, id_restaurante, total, productos } = req.body;
    let connection;

    try {
        const usuarioTarget = id_usuario ?? null;
        const nodoTarget = parseInt(id_region, 10) || 1;
        const restauranteTarget = id_restaurante ? String(id_restaurante) : '1';
        const totalTarget = total !== undefined ? parseFloat(total) : 0.0;

        if (!usuarioTarget) {
            return res.status(400).json({ 
                success: false, 
                message: "El campo 'id_usuario' es obligatorio y no puede ser undefined." 
            });
        }

        connection = await getNodoConnection(nodoTarget);
        await connection.beginTransaction();

        const queryPedido = `
            INSERT INTO pedidos (id_usuario, id_region, id_restaurante, total, estado) 
            VALUES (?, ?, ?, ?, 'pendiente')
        `;
        
        const [resultPedido] = await connection.execute(queryPedido, [
            usuarioTarget, 
            nodoTarget, 
            restauranteTarget, 
            totalTarget
        ]);

        const id_pedido = resultPedido.insertId;

        if (productos && Array.isArray(productos) && productos.length > 0) {
            const queryDetalle = `
                INSERT INTO pedido_detalles (id_pedido, id_producto, cantidad, precio_unitario) 
                VALUES (?, ?, ?, ?)
            `;
            for (const prod of productos) {
                await connection.execute(queryDetalle, [
                    id_pedido, 
                    prod.id_producto ?? null, 
                    prod.cantidad ?? 1, 
                    prod.precio_unitario ?? 0.0
                ]);
            }
        }

        await connection.commit();

        const nuevoPedido = {
            id_pedido,
            id_usuario: usuarioTarget,
            id_region: nodoTarget,
            id_restaurante: restauranteTarget,
            total: totalTarget,
            estado: 'pendiente'
        };

        // ⚡ Notificar ALTA EN TIEMPO REAL a la sala de repartidores
        io.to(`region_${nodoTarget}`).emit('nuevo_pedido', nuevoPedido);

        res.status(201).json({ 
            success: true, 
            message: "Pedido registrado y enviado.",
            id_pedido,
            estado: 'pendiente'
        });

    } catch (error) {
        if (connection) await connection.rollback();
        console.error(`[ERROR CREAR PEDIDO]: ${error.message}`);
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

// 🟡 CONSULTA: Obtener Pedidos Activos por Región (Repartidor)
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

// 🔵 MODIFICACIÓN: Cambiar Estado de Pedido (Tiempo Real)
app.put('/api/pedidos/actualizar-estado', async (req, res) => {
    const { id_pedido, id_region, estado, nuevoEstado, id_repartidor } = req.body;
    const estadoFinal = estado || nuevoEstado;
    let connection;

    try {
        const nodoTarget = parseInt(id_region, 10) || 1;
        connection = await getNodoConnection(nodoTarget);

        const query = `
            UPDATE pedidos 
            SET estado = ?, 
                id_repartidor = COALESCE(?, id_repartidor) 
            WHERE id_pedido = ?
        `;
        await connection.execute(query, [estadoFinal, id_repartidor || null, id_pedido]);

        const datosActualizados = { id_pedido, estado: estadoFinal, id_repartidor };

        // 💥 EVENTOS SOCKET:
        io.to(`pedido_${id_pedido}`).emit('cambio_estado_pedido', datosActualizados);
        io.to(`region_${nodoTarget}`).emit('actualizacion_region', datosActualizados);
        io.to(`region_${nodoTarget}`).emit('actualizacion_pedido_region', datosActualizados);

        res.status(200).json({ 
            success: true, 
            message: `Estado actualizado a '${estadoFinal}' exitosamente.`,
            estado: estadoFinal 
        });

    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

// 🔴 BAJA: Cancelar Pedido
app.delete('/api/pedidos/cancelar/:id_region/:id_pedido', async (req, res) => {
    const { id_region, id_pedido } = req.params;
    let connection;

    try {
        const nodoTarget = parseInt(id_region, 10) || 1;
        connection = await getNodoConnection(nodoTarget);

        await connection.execute(
            `UPDATE pedidos SET estado = 'cancelado' WHERE id_pedido = ?`,
            [id_pedido]
        );

        io.to(`region_${nodoTarget}`).emit('actualizacion_region', { id_pedido, estado: 'cancelado' });

        res.status(200).json({ success: true, message: "Pedido cancelado correctamente." });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

// ====================================================================
// 🍔 PRODUCTOS / MENÚ (ADMIN SUCURSAL - CRUD COMPLETO)
// ====================================================================

// Obtener Productos por Región
app.get('/api/productos/:id_region', async (req, res) => {
    const { id_region } = req.params;
    let connection;

    try {
        connection = await getNodoConnection(parseInt(id_region, 10));
        const [rows] = await connection.execute(`SELECT * FROM productos ORDER BY id_producto DESC`);
        res.status(200).json({ success: true, productos: rows });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

// 🟢 ALTA: Crear Producto
app.post('/api/productos/crear', async (req, res) => {
    const { id_region, nombre, descripcion, precio, id_restaurante } = req.body;
    let connection;

    try {
        const nodoTarget = parseInt(id_region, 10) || 1;
        connection = await getNodoConnection(nodoTarget);

        const query = `
            INSERT INTO productos (id_region, id_restaurante, nombre, descripcion, precio) 
            VALUES (?, ?, ?, ?, ?)
        `;
        const [result] = await connection.execute(query, [
            nodoTarget,
            id_restaurante || '1',
            nombre,
            descripcion || '',
            precio
        ]);

        res.status(201).json({
            success: true,
            message: "Producto creado exitosamente.",
            id_producto: result.insertId
        });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

// 🔵 MODIFICACIÓN: Editar Producto
app.put('/api/productos/editar', async (req, res) => {
    const { id_producto, id_region, nombre, descripcion, precio } = req.body;
    let connection;

    try {
        connection = await getNodoConnection(parseInt(id_region, 10));

        const query = `
            UPDATE productos 
            SET nombre = ?, descripcion = ?, precio = ? 
            WHERE id_producto = ?
        `;
        await connection.execute(query, [nombre, descripcion, precio, id_producto]);

        res.status(200).json({ success: true, message: "Producto actualizado correctamente." });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

// 🔴 BAJA: Eliminar Producto
app.delete('/api/productos/eliminar/:id_region/:id_producto', async (req, res) => {
    const { id_region, id_producto } = req.params;
    let connection;

    try {
        connection = await getNodoConnection(parseInt(id_region, 10));
        await connection.execute(`DELETE FROM productos WHERE id_producto = ?`, [id_producto]);

        res.status(200).json({ success: true, message: "Producto eliminado correctamente." });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

// ====================================================================
// 👥 USUARIOS (ADMIN GENERAL - CRUD COMPLETO)
// ====================================================================

// Obtener Usuarios por Región
app.get('/api/usuarios/:id_region', async (req, res) => {
    const { id_region } = req.params;
    let connection;

    try {
        connection = await getNodoConnection(parseInt(id_region, 10));
        const [rows] = await connection.execute(
            `SELECT id_usuario, id_region, nombre, email, rol FROM usuarios ORDER BY nombre ASC`
        );
        res.status(200).json({ success: true, usuarios: rows });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

// 🔵 MODIFICACIÓN: Editar Usuario
app.put('/api/usuarios/editar', async (req, res) => {
    const { id_usuario, id_region, nombre, email, rol } = req.body;
    let connection;

    try {
        connection = await getNodoConnection(parseInt(id_region, 10));

        const query = `
            UPDATE usuarios 
            SET nombre = ?, email = ?, rol = ? 
            WHERE id_usuario = ?
        `;
        await connection.execute(query, [nombre, email, rol, id_usuario]);

        res.status(200).json({ success: true, message: "Usuario actualizado correctamente." });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

// 🔴 BAJA: Eliminar Usuario
app.delete('/api/usuarios/eliminar/:id_region/:id_usuario', async (req, res) => {
    const { id_region, id_usuario } = req.params;
    let connection;

    try {
        connection = await getNodoConnection(parseInt(id_region, 10));
        await connection.execute(`DELETE FROM usuarios WHERE id_usuario = ?`, [id_usuario]);

        res.status(200).json({ success: true, message: "Usuario eliminado correctamente." });
    } catch (error) {
        res.status(500).json({ success: false, message: error.message });
    } finally {
        if (connection) await connection.end();
    }
});

// ====================================================================
// 🚀 ARRANCAR SERVIDOR EN IP GLOBAL '0.0.0.0'
// ====================================================================
const PORT = process.env.PORT || 3000;

server.listen(PORT, '0.0.0.0', () => {
    const interfaces = os.networkInterfaces();
    let localIp = '192.168.100.17';

    for (const name of Object.keys(interfaces)) {
        for (const net of interfaces[name]) {
            if (net.family === 'IPv4' && !net.internal) {
                localIp = net.address;
                break;
            }
        }
    }

    console.log(`\n🚀 EatFast Distributed Engine v3.0 corriendo correctamente:`);
    console.log(`   - Desde PC Local: http://localhost:${PORT}`);
    console.log(`   - Desde Celular/Red: http://${localIp}:${PORT}\n`);
    console.log(`📌 Usa esta URL base en Flutter: http://${localIp}:${PORT}/api`);
});