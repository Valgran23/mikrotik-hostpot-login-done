const express = require('express');
const { RouterOSClient } = require('routeros-client');
const app = express();

app.use(express.json());

// --- DATABASE SEMENTARA (MEMORY STORE) ---
let users = [
    { name: 'Aksyra', email: 'aksyra042@gmail.com', username: 'Valgran', status: 'approved', timestamp: Date.now() },
    { name: 'Henok', email: 'enoktambunan5@gmail.com', username: 'Henok', status: 'approved', timestamp: Date.now() },
    { name: 'lutpi', email: 'luthfibrilian572@gmail.com', username: 'lutpi', status: 'approved', timestamp: Date.now() },
    { name: 'Moreno Wijaya Pakpahan', email: 'mwijayapakpahan@gmail.com', username: 'moreno', status: 'approved', timestamp: Date.now() },
    { name: 'Mbud', email: 'mbud@gmail.com', username: 'Mbus', status: 'approved', timestamp: Date.now() },
    { name: 'habib', email: 'bib@gmail.com', username: 'habib', status: 'approved', timestamp: Date.now() },
    { name: 'aidil', email: 'aidil@nau.go.id', username: 'aidil', status: 'approved', timestamp: Date.now() }
];

// Konfigurasi MikroTik (diambil dari Environment Variables Vercel)
const getMikrotikClient = () => {
    return new RouterOSClient({
        host: process.env.MIKROTIK_HOST,
        port: parseInt(process.env.MIKROTIK_PORT || '8728'),
        user: process.env.MIKROTIK_USER,
        password: process.env.MIKROTIK_PASSWORD,
        timeout: 10
    });
};

// --- ENDPOINT ADMIN LOGIN ---
app.post('/api/admin/login', (req, res) => {
    const { password } = req.body;
    const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'admin123';

    if (password === ADMIN_PASSWORD) {
        return res.json({ success: true, message: 'Login berhasil' });
    }
    res.json({ success: false, message: 'Password salah!' });
});

// --- ENDPOINT GET ALL USERS + LIVE TRAFFIC MIKROTIK ---
app.get('/api/users', async (req, res) => {
    try {
        let mtUsers = [];
        let mtActiveUsers = [];

        // Hubungkan ke MikroTik jika host terisi
        if (process.env.MIKROTIK_HOST) {
            try {
                const client = getMikrotikClient();
                const api = await client.connect();

                const userMenu = api.menu('/ip/hotspot/user');
                const activeMenu = api.menu('/ip/hotspot/active');

                mtUsers = await userMenu.get();
                mtActiveUsers = await activeMenu.get();

                await client.close();
            } catch (connErr) {
                console.error("Gagal konek ke MikroTik:", connErr.message);
            }
        }

        // Gabungkan data user dengan data traffic MikroTik
        const responseUsers = users.map(user => {
            const mtUser = mtUsers.find(u => u.name === user.username);
            const activeUser = mtActiveUsers.find(u => u.user === user.username);

            let bytesIn = 0;
            let bytesOut = 0;
            let uptime = 'Off';

            if (mtUser) {
                bytesIn = parseInt(mtUser['bytes-in'] || 0);   // Upload
                bytesOut = parseInt(mtUser['bytes-out'] || 0); // Download
                uptime = mtUser['uptime'] || 'Off';
            }

            if (activeUser) {
                uptime = `${activeUser.uptime} (Online)`;
            }

            return {
                ...user,
                bytesIn,
                bytesOut,
                uptime
            };
        });

        res.json({ success: true, users: responseUsers });
    } catch (error) {
        console.error("Error pada /api/users:", error);
        res.status(500).json({ success: false, message: 'Gagal mengambil data user' });
    }
});

// --- ENDPOINT APPROVE USER ---
app.post('/api/approve', async (req, res) => {
    const { username } = req.body;
    const user = users.find(u => u.username === username);

    if (user) {
        user.status = 'approved';

        if (process.env.MIKROTIK_HOST) {
            try {
                const client = getMikrotikClient();
                const api = await client.connect();
                
                await api.menu('/ip/hotspot/user').add({
                    name: username,
                    password: user.password || '123456',
                    profile: 'default'
                });

                await client.close();
            } catch (err) {
                console.error("Gagal tambah user di MikroTik:", err.message);
            }
        }

        return res.json({ success: true, message: `User ${username} berhasil diapprove` });
    }

    res.json({ success: false, message: 'User tidak ditemukan' });
});

// --- ENDPOINT DELETE USER ---
app.delete('/api/users/:username', async (req, res) => {
    const { username } = req.params;
    users = users.filter(u => u.username !== username);

    if (process.env.MIKROTIK_HOST) {
        try {
            const client = getMikrotikClient();
            const api = await client.connect();

            const userMenu = api.menu('/ip/hotspot/user');
            const target = await userMenu.get({ name: username });

            if (target && target.length > 0) {
                await userMenu.remove(target[0]['.id']);
            }

            await client.close();
        } catch (err) {
            console.error("Gagal hapus user di MikroTik:", err.message);
        }
    }

    res.json({ success: true, message: `User ${username} berhasil dihapus` });
});

module.exports = app;