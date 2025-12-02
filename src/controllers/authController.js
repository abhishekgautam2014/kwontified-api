const jwt = require('jsonwebtoken');
const bcrypt = require('bcryptjs');
const { User } = require('../../models');

const login = async (req, res) => {
  const { username, password } = req.body;
  console.log('Login attempt for user:', username);
  // Basic input validation
  if (!username || !password) {
    return res
      .status(400)
      .json({ message: 'Username and password are required.' });
  }

  try {
    // Fetch user from the database
    const user = await User.findOne({ where: { username } });

    if (!user) {
      return res.status(401).json({
        success: false,
        message: 'Invalid username or password.',
      });
    }

    // Compare the provided password with the stored hash
    const isMatch = await bcrypt.compare(password, user.password);

    if (!isMatch) {
      return res.status(401).json({
        success: false,
        message: 'Invalid username or password.',
      });
    }

    // If the password is correct, create a JWT
    const token = jwt.sign(
      { id: user.id, username: user.username, account_id: user.account_id },
      process.env.JWT_SECRET,
      {
        expiresIn: '24h', // Token expires in 1 hour
      }
    );

    res.status(200).json({
      success: true,
      message: 'Login successful.',
      token,
      user,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: 'Internal server error.',
    });
  }
};

const register = async (req, res) => {
  const { username, password, email, account_id, account_name, name, description } = req.body;

  if (!username || !password) {
    return res
      .status(400)
      .json({ message: 'Username and password are required.' });
  }

  try {
    const userExists = await User.findOne({ where: { username } });
    if (userExists) {
      return res
        .status(400)
        .json({ message: 'Username already exists.' });
    }

    await User.create({
      username,
      password,
      email,
      account_id,
      account_name,
      name,
      description,
    });

    res.status(201).json({
      message: 'User created successfully.',
    });
  } catch (error) {
    res.status(500).json({
      message: 'Internal server error.',
    });
  }
};

// ----------------------- CHANGE PASSWORD -----------------------
const changePassword = async (req, res) => {
  const userId = req.user.id; // comes from JWT middleware
  const { oldPassword, newPassword } = req.body;

  if (!oldPassword || !newPassword) {
    return res
      .status(400)
      .json({ message: 'Old & new password are required.' });
  }

  try {
    // 1. Fetch user from DB
    const user = await User.findByPk(userId);
    if (!user) return res.status(404).json({ message: 'User not found.' });

    // 2. Compare old password
    const isMatch = await bcrypt.compare(oldPassword, user.password);
    if (!isMatch)
      return res
        .status(401)
        .json({ message: 'Old password is incorrect.' });

    // 3. Update password in DB
    user.password = newPassword;
    await user.save();

    res.status(200).json({
      message: 'Password updated successfully.',
    });
  } catch (error) {
    res.status(500).json({ message: 'Internal server error.' });
  }
};

module.exports = {
  login,
  register,
  changePassword,
};
