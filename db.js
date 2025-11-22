// db.js
const sqlite3 = require("sqlite3").verbose();
const db = new sqlite3.Database("./accounts.db");

// Your data array
const accounts = [
	{ account_id: 1314390, account_name: "Fair Harbor Clothing Seller US" },
	{ account_id: 1229370, account_name: "Live Tinted Inc. Seller US" },
	{ account_id: 1229600, account_name: "RADIUS Corporation  Seller 3P US" },
	{ account_id: 1214910, account_name: "Live Light Bags Seller CA" },
	{ account_id: 2501580, account_name: "TULA Skincare UK Seller UK" },
	{
		account_id: 1230790,
		account_name: "Vilebrequin Official Store Seller US",
	},
	{ account_id: 1230810, account_name: "Vilebrequin Seller UK" },
	{ account_id: 2670360, account_name: "Alex Evenings Vendor US" },
	{ account_id: 2093870, account_name: "Flojos Vendor US" },
	{ account_id: 1228540, account_name: "KORRES Vendor US" },
	{
		account_id: 1886420,
		account_name: "Pacific World Corporation Vendor US",
	},
	{ account_id: 1227690, account_name: "Electric Visual Seller US" },
	{ account_id: 1228250, account_name: "EVE LOM Skincare Seller US" },
	{ account_id: 1228360, account_name: "Everyday Oil Seller US" },
	{ account_id: 1230010, account_name: "France Luxe Seller US" },
	{ account_id: 2812520, account_name: "Seacret Seller CA" },
	{ account_id: 1361560, account_name: "Trish McEvoy Seller UK" },
	{ account_id: 1231330, account_name: "Volcom Inc Seller CA" },
	{ account_id: 1231310, account_name: "Volcom Inc Seller US" },
	{ account_id: 1214930, account_name: "Live Light Bags Seller MX" },
	{ account_id: 1230830, account_name: "Vilebrequin Seller IT" },
	{ account_id: 1228470, account_name: "Kipling Vendor US" },
	{ account_id: 1229560, account_name: "RADIUS Corporation Vendor 1P US" },
	{ account_id: 1228030, account_name: "Ely Cattleman Seller US" },
	{ account_id: 1228580, account_name: "Greek Beauty Seller US" },
	{ account_id: 1214950, account_name: "Kipling Commerce Seller US" },
	{ account_id: 1390770, account_name: "Liberated Brands Seller US" },
	{ account_id: 1229490, account_name: "Par West Seller US" },
	{ account_id: 1361810, account_name: "Spyder US Seller US" },
	{ account_id: 2434390, account_name: "TULA Skin Care Seller CA" },
	{ account_id: 1230310, account_name: "TULA Skin Care Seller US" },
	{ account_id: 1230170, account_name: "Trish McEvoy Seller US" },
	{ account_id: 3386240, account_name: "Divi Scalp Care Seller CA" },
	{ account_id: 1231550, account_name: "Drink Gist Seller US" },
	{ account_id: 1783040, account_name: "Fair Harbor Seller CA" },
	{ account_id: 2642000, account_name: "The Better Menopause Seller US" },
	{ account_id: 2260430, account_name: "The Fullest Seller US" },
	{ account_id: 1230930, account_name: "Vilebrequin Seller FR" },
	{ account_id: 2237590, account_name: "Flojos Shoes Vendor US" },
	{ account_id: 1882350, account_name: "PWC Vendor US" },
	{ account_id: 2670380, account_name: "S.L. Fashions Vendor US" },
	{ account_id: 1256730, account_name: "Volcom Inc. (Footwear) Vendor US" },
	{ account_id: 1231250, account_name: "Volcom Juniors Vendor US" },
	{ account_id: 1231270, account_name: "Volcom Young Men's Vendor US" },
	{ account_id: 3441680, account_name: "Blue Sage Accessories Seller US" },
	{ account_id: 2503620, account_name: "Divi Scalp Care Seller US" },
	{ account_id: 1228560, account_name: "KORRES Seller CA" },
	{ account_id: 2147440, account_name: "Pelagic Seller US" },
	{ account_id: 2812720, account_name: "Seacret Seller US" },
	{ account_id: 2485660, account_name: "StickerBeans Seller US" },
	{ account_id: 1488110, account_name: "Eve Lom UK Seller UK" },
	{ account_id: 1230770, account_name: "Vilebrequin Seller DE" },
	{ account_id: 1230870, account_name: "Vilebrequin Seller ES" },
	{ account_id: 1229760, account_name: "Liberated Spyder LLC Vendor US" },
	{ account_id: 1228230, account_name: "Space Brands Limited UK Vendor UK" },
	{ account_id: 1231370, account_name: "Volcom Boys 2-7 Vendor US" },
];

db.serialize(() => {
	db.run(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      account_id INTEGER UNIQUE,
      account_name TEXT,
      password TEXT DEFAULT 'Admin#123'
    )
  `);

	const stmt = db.prepare(
		"INSERT OR IGNORE INTO users (account_id, account_name, password) VALUES (?, ?, 'Admin#123')"
	);

	accounts.forEach((acc) => {
		stmt.run(acc.account_id, acc.account_name, (err) => {
			if (err) {
				console.log("Insert error:", err.message);
			}
		});
	});

	stmt.finalize();
});

module.exports = db;
