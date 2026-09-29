const { Redis } = require('@upstash/redis');

class StorageConfig {
  constructor() {
    this.redisKey = 'hotspot_users_data';
    this.redis = Redis.fromEnv();
    this.data = {
      users: []
    };
  }

  async initialize() {
    try {
      // Ambil data users dari Upstash Redis Cloud
      const existingData = await this.redis.get(this.redisKey);
      
      if (existingData) {
        this.data = typeof existingData === 'string' ? JSON.parse(existingData) : existingData;
        console.log('Storage loaded successfully from Upstash Redis');
      } else {
        // Jika Redis masih kosong, simpan struktur awal
        await this.saveData();
        console.log('Storage initialized successfully on Upstash Redis');
      }
    } catch (error) {
      console.error('Error initializing storage:', error);
      throw error;
    }
  }

  async saveData() {
    try {
      // Simpan seluruh object this.data ke Upstash Redis
      await this.redis.set(this.redisKey, JSON.stringify(this.data));
    } catch (error) {
      console.error('Error saving data:', error);
      throw error;
    }
  }

  async addUser(userData) {
    try {
      await this.initialize(); // Sinkronisasi data terbaru dari Redis
      
      const timestamp = new Date().toISOString();
      const newUser = {
        id: Date.now().toString(),
        timestamp,
        name: userData.name,
        email: userData.email,
        phone: userData.phone,
        username: userData.username,
        password: userData.password,
        status: 'pending',
        approvedAt: '',
        approvedBy: ''
      };

      this.data.users.push(newUser);
      await this.saveData();
      
      return newUser;
    } catch (error) {
      console.error('Error adding user:', error);
      throw error;
    }
  }

  async getAllUsers() {
    try {
      await this.initialize(); // Sinkronisasi data terbaru dari Redis
      return this.data.users;
    } catch (error) {
      console.error('Error getting users:', error);
      throw error;
    }
  }

  async approveUser(username, adminName) {
    try {
      await this.initialize(); // Sinkronisasi data terbaru dari Redis
      const userIndex = this.data.users.findIndex(u => u.username === username);
      
      if (userIndex === -1) {
        throw new Error('User not found');
      }
      
      this.data.users[userIndex].status = 'approved';
      this.data.users[userIndex].approvedAt = new Date().toISOString();
      this.data.users[userIndex].approvedBy = adminName;
      
      await this.saveData();
      
      return this.data.users[userIndex];
    } catch (error) {
      console.error('Error approving user:', error);
      throw error;
    }
  }

  async rejectUser(username, adminName) {
    try {
      await this.initialize(); // Sinkronisasi data terbaru dari Redis
      const userIndex = this.data.users.findIndex(u => u.username === username);
      
      if (userIndex === -1) {
        throw new Error('User not found');
      }
      
      this.data.users[userIndex].status = 'rejected';
      this.data.users[userIndex].approvedAt = new Date().toISOString();
      this.data.users[userIndex].approvedBy = adminName;
      
      await this.saveData();
      
      return this.data.users[userIndex];
    } catch (error) {
      console.error('Error rejecting user:', error);
      throw error;
    }
  }

  async getUserByUsername(username) {
    try {
      await this.initialize(); // Sinkronisasi data terbaru dari Redis
      return this.data.users.find(u => u.username === username) || null;
    } catch (error) {
      console.error('Error getting user:', error);
      throw error;
    }
  }

  async deleteUser(username) {
    try {
      await this.initialize(); // Sinkronisasi data terbaru dari Redis
      const userIndex = this.data.users.findIndex(u => u.username === username);
      
      if (userIndex === -1) {
        throw new Error('User not found');
      }
      
      this.data.users.splice(userIndex, 1);
      await this.saveData();
      
      return true;
    } catch (error) {
      console.error('Error deleting user:', error);
      throw error;
    }
  }
}

module.exports = new StorageConfig();