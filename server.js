require('dotenv').config();
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const path = require('path');
const storageConfig = require('./config/storage');
const mikrotikConfig = require('./config/mikrotik');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, 'public')));

// Routes Static File
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'login.html'));
});

app.get('/register', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'register.html'));
});

app.get('/admin', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'admin.html'));
});

// API Routes
// Register new user
app.post('/api/register', async (req, res) => {
  try {
    const { name, email, phone, username, password } = req.body;
    
    // Validate input
    if (!name || !email || !phone || !username || !password) {
      return res.status(400).json({ success: false, message: 'All fields are required' });
    }
    
    // Check if username already exists
    const existingUser = await storageConfig.getUserByUsername(username);
    if (existingUser) {
      return res.status(400).json({ success: false, message: 'Username already exists' });
    }
    
    // Add user to storage
    await storageConfig.addUser({ name, email, phone, username, password });
    
    res.json({ success: true, message: 'Registration successful. Please wait for admin approval.' });
  } catch (error) {
    console.error('Registration error:', error);
    res.status(500).json({ success: false, message: 'Registration failed' });
  }
});

// Get all users (admin only) - DIPERBARUI DENGAN STATISTIK MIKROTIK
app.get('/api/users', async (req, res) => {
  try {
    const users = await storageConfig.getAllUsers();
    
    // Ambil data statistik aktif & user dari MikroTik
    let mikrotikStats = [];
    if (mikrotikConfig.getUsersStats && typeof mikrotikConfig.getUsersStats === 'function') {
      mikrotikStats = await mikrotikConfig.getUsersStats();
    }

    // Gabungkan data user storage dengan statistik MikroTik
    const enrichedUsers = users.map(user => {
      const stats = mikrotikStats.find(s => s.name === user.username || s.user === user.username) || {};
      
      return {
        ...user,
        bytesIn: parseInt(stats['bytes-in'] || stats.bytesIn || 0),
        bytesOut: parseInt(stats['bytes-out'] || stats.bytesOut || 0),
        uptime: stats.uptime || 'Off'
      };
    });

    res.json({ success: true, users: enrichedUsers });
  } catch (error) {
    console.error('Error getting users:', error);
    res.status(500).json({ success: false, message: 'Failed to get users' });
  }
});

// Approve user (admin only)
app.post('/api/approve', async (req, res) => {
  try {
    const { username, adminName } = req.body;
    
    if (!username || !adminName) {
      return res.status(400).json({ success: false, message: 'Username and admin name are required' });
    }
    
    // Update user status in storage
    await storageConfig.approveUser(username, adminName);
    
    // Add user to Mikrotik hotspot
    const userData = await storageConfig.getUserByUsername(username);
    await mikrotikConfig.addUserToHotspot(userData.username, userData.password);
    
    res.json({ success: true, message: 'User approved successfully' });
  } catch (error) {
    console.error('Approval error:', error);
    res.status(500).json({ success: false, message: 'Approval failed' });
  }
});

// Reject user (admin only)
app.post('/api/reject', async (req, res) => {
  try {
    const { username, adminName } = req.body;
    
    if (!username || !adminName) {
      return res.status(400).json({ success: false, message: 'Username and admin name are required' });
    }
    
    await storageConfig.rejectUser(username, adminName);
    
    res.json({ success: true, message: 'User rejected successfully' });
  } catch (error) {
    console.error('Rejection error:', error);
    res.status(500).json({ success: false, message: 'Rejection failed' });
  }
});

// Delete user (admin only)
app.delete('/api/users/:username', async (req, res) => {
  try {
    const { username } = req.params;
    
    await storageConfig.deleteUser(username);
    await mikrotikConfig.removeUserFromHotspot(username);
    
    res.json({ success: true, message: 'User deleted successfully' });
  } catch (error) {
    console.error('Deletion error:', error);
    res.status(500).json({ success: false, message: 'Deletion failed' });
  }
});

// Validate login
app.post('/api/login', async (req, res) => {
  try {
    const { username, password } = req.body;
    
    if (!username || !password) {
      return res.status(400).json({ success: false, message: 'Username and password are required' });
    }
    
    // Get user from storage
    const user = await storageConfig.getUserByUsername(username);
    
    if (!user) {
      return res.status(404).json({ success: false, message: 'User not found' });
    }
    
    // Check if password matches
    if (user.password !== password) {
      return res.status(401).json({ success: false, message: 'Invalid password' });
    }
    
    // Check if user is approved
    if (user.status !== 'approved') {
      return res.status(403).json({ success: false, message: 'User not approved yet' });
    }
    
    // Validate with Mikrotik
    const mikrotikResult = await mikrotikConfig.validateLogin(username, password);
    
    if (mikrotikResult.success) {
      res.json({ success: true, message: 'Login successful', user: { name: user.name, username: user.username } });
    } else {
      res.status(401).json({ success: false, message: mikrotikResult.message });
    }
  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ success: false, message: 'Login failed' });
  }
});

// Check user status
app.get('/api/status/:username', async (req, res) => {
  try {
    const { username } = req.params;
    const user = await storageConfig.getUserByUsername(username);
    
    if (!user) {
      return res.status(404).json({ success: false, message: 'User not found' });
    }
    
    res.json({ success: true, status: user.status });
  } catch (error) {
    console.error('Status check error:', error);
    res.status(500).json({ success: false, message: 'Status check failed' });
  }
});

// Admin authentication
app.post('/api/admin/login', (req, res) => {
  const { password } = req.body;
  
  if (password === process.env.ADMIN_PASSWORD) {
    res.json({ success: true, message: 'Admin login successful' });
  } else {
    res.status(401).json({ success: false, message: 'Invalid admin password' });
  }
});

// Export app handler untuk Vercel Serverless
module.exports = app;

// Jalankan server jika running di lokal (npm start / node server.js)
if (process.env.NODE_ENV !== 'production') {
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Server running on http://localhost:${PORT}`);
  });
}