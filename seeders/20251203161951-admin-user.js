'use strict';
const bcrypt = require('bcryptjs');

module.exports = {
  up: async (queryInterface, Sequelize) => {
    const hashedPassword = await bcrypt.hash('Admin#123', 10);
    
    await queryInterface.bulkInsert('Users', [{
      account_id: 0, // Or some other unique identifier for admin
      account_name: 'admin',
      username: 'admin',
      email: 'admin@gmail.com',
      name: 'Kwontified Admin',
      description: 'Administrator account',
      password: hashedPassword,
      createdAt: new Date(),
      updatedAt: new Date()
    }], {});
  },

  down: async (queryInterface, Sequelize) => {
    await queryInterface.bulkDelete('Users', { username: 'admin' }, {});
  }
};