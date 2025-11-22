const getAllAccounts = `
SELECT 
account_id, account_name
FROM 
  \`intentwise_ecommerce_graph.account_summary\`
GROUP BY 
  account_id, account_name
`;

module.exports = {
	getAllAccounts,
};
