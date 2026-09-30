const { RouterOSAPI } = require('routeros-api');

class MikrotikConfig {
  constructor() {
    // Membaca dari MIKROTIK_HOST atau MIKROTIK_IP
    this.host = process.env.MIKROTIK_HOST || process.env.MIKROTIK_IP;
    // Membaca dari MIKROTIK_USER atau MIKROTIK_API_USER
    this.user = process.env.MIKROTIK_USER || process.env.MIKROTIK_API_USER;
    // Membaca dari MIKROTIK_PASSWORD atau MIKROTIK_API_PASSWORD
    this.password = process.env.MIKROTIK_PASSWORD || process.env.MIKROTIK_API_PASSWORD;
    // Membaca dari MIKROTIK_PORT atau MIKROTIK_API_PORT (Default 8728)
    this.port = parseInt(process.env.MIKROTIK_PORT || process.env.MIKROTIK_API_PORT) || 8728;
  }

  // Fungsi untuk membuat koneksi ke MikroTik
  async connect() {
    const conn = new RouterOSAPI({
      host: this.host,
      user: this.user,
      password: this.password,
      port: this.port,
      timeout: 10
    });

    try {
      await conn.connect();
      return conn;
    } catch (error) {
      console.error('Gagal terhubung ke MikroTik:', error.message);
      return null;
    }
  }

  async login() {
    const conn = await this.connect();
    if (conn) {
      console.log('Berhasil terhubung ke MikroTik via API!');
      await conn.close();
      return true;
    }
    return false;
  }

  // Fungsi otomatis menambahkan user ke Hotspot MikroTik saat Approve
  async addUserToHotspot(username, password, profile = 'default') {
    const conn = await this.connect();
    if (!conn) {
      console.error('Gagal menambah user: Tidak dapat terhubung ke MikroTik');
      return { success: false, message: 'Koneksi ke MikroTik gagal' };
    }

    try {
      // Perintah API MikroTik untuk menambah hotspot user
      await conn.write('/ip/hotspot/user/add', [
        `=name=${username}`,
        `=password=${password}`,
        `=profile=${profile}`
      ]);

      console.log(`Berhasil menambahkan user ${username} ke MikroTik Hotspot!`);
      await conn.close();
      return { success: true, message: 'User added to hotspot' };
    } catch (error) {
      console.error('Error adding user to hotspot:', error);
      try { await conn.close(); } catch (e) {}
      return { success: false, message: error.message };
    }
  }

  async removeUserFromHotspot(username) {
    const conn = await this.connect();
    if (!conn) return { success: false, message: 'Koneksi gagal' };

    try {
      const users = await conn.write('/ip/hotspot/user/print', [
        `?name=${username}`
      ]);

      if (users && users.length > 0) {
        const userId = users[0]['.id'];
        await conn.write('/ip/hotspot/user/remove', [
          `=.id=${userId}`
        ]);
        console.log(`Berhasil menghapus user ${username} dari MikroTik.`);
      }

      await conn.close();
      return { success: true, message: 'User removed from hotspot' };
    } catch (error) {
      console.error('Error removing user from hotspot:', error);
      try { await conn.close(); } catch (e) {}
      return { success: false, message: error.message };
    }
  }

  async validateLogin(username, password) {
    return { success: true, message: 'Login valid' };
  }

  // FUNGSI BARU: Ambil statistik akumulasi user & statistik user aktif dari MikroTik
  async getUsersStats() {
    const conn = await this.connect();
    if (!conn) return [];

    try {
      // Ambil daftar seluruh user hotspot (termasuk total bytes-in/out dan uptime akumulatif)
      const hotspotUsers = await conn.write('/ip/hotspot/user/print');
      
      // Ambil daftar user yang sedang aktif online di hotspot
      const activeUsers = await conn.write('/ip/hotspot/active/print');

      await conn.close();

      // Gabungkan data agar user aktif mendapatkan statistik live realtime
      return hotspotUsers.map(user => {
        const active = activeUsers.find(act => act.user === user.name);
        return {
          name: user.name,
          'bytes-in': active ? active['bytes-in'] : (user['bytes-in'] || 0),
          'bytes-out': active ? active['bytes-out'] : (user['bytes-out'] || 0),
          uptime: active ? active.uptime : (user.uptime || 'Off')
        };
      });
    } catch (error) {
      console.error('Error fetching Mikrotik statistics:', error);
      try { await conn.close(); } catch (e) {}
      return [];
    }
  }
}

module.exports = new MikrotikConfig();