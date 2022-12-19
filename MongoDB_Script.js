// Finding the total revenue of each division over the months and sort in descending order. Analytical purpose: The firm wants to know which division is leading and the CEO would like to allocate more funding and resources to the divisions that can do better.

db.revenue.aggregate([
{
    $group: {
        _id: "$div_id", 
        total_revenue: {
            $sum:"$revenue"
            }
        }
},
{
    $sort:{ "total_revenue" : -1}
}
]);

// The firm wants to give certain free perks to common individual clients who have total savings in the bank between 10,000 USD and 30,000 USD. List the individual’s details.

db.commoner.find({
    $and: [
 {"total_savings":{ $gt : 10000 }},
 {"total_savings":{ $lt : 30000 }}
 ]
});

// Find out how many employees are working under each manager. Do not list those employees who do have a manager.

db.employee.aggregate([
{ $match: {manager_id : {$exists: true, $ne: null}} },
{ $group: { _id: "$manager_id", 
    num_of_employees:{ $count:{} }}}
]);

// Identify the cost price of each security. Cost price = cost price per share * quantity. Total cost price is calculated to get the amount the investment bank has to pay to the corporates after the securities have been sold. 

db.securities.aggregate([{
    $project: {
        security_id: 1, 
        total_cost_price: {$multiply: ["$price_per_share", "$total_quantity"]}
        }
}])


// Get the details of the investment bankers who have underwritten till now. The firm wants to award the bankers for their service.

var underwriters = db.underwriting.distinct("IBanker_id")
db.employee.find({"ID": {"$in": underwriters}});

// Identify how much each AM client has paid to the firm in terms of fees for the services and the profit share.

db.AMprovision.aggregate([
{
    $group: {
        _id: "$AMclient_id",
        total_profitshare: {$sum: "$profitshare"},
        total_fees: {$sum: "$fee"}
    }
},
{
    $project: {
        AMclient_id: 1,
        total_fee: "$fee",
        total_profitshare: "$profitshare",
        total_pay: {$add: ["$total_fees", "$total_profitshare"]}
    }
}]);
