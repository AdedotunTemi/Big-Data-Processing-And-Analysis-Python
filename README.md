### BIG DATA PROCESSING AND ANALYSIS IMPLEMENTATION
The dataset file comprises a total of 21 individual files. The files embody a standard data warehouse schema of dimension (Dim) and fact (Fact) tables. Dimension tables, such as DimAccount, DimCurrency, DimCustomer, DimDate, DimGeography, DimProduct, and others, include descriptive attributes for different business entities, including accounts, currencies, customers, and items. The fact tables, such as FactCallCenter, FactCurrencyRate, FactFinance, FactInternetSale, and FactSalesTargets, store quantifiable and measurable data pertaining to various business activities, including financial transactions, sales information, and operational metrics. This schema facilitates intricate analytical queries for corporate intelligence. In order to comprehend the links, it is imperative to ascertain the pivotal columns that establish connections between these tables. Typically, in a data warehouse schema, dimension tables like DimCustomer and DimProduct are linked to fact tables like FactInternetSales through foreign keys.

Here's the relationship structure:
• DimCustomer is linked to FactInternetSales via CustomerKey.
• DimProduct is linked to FactInternetSales via ProductKey.
• Other dimension tables may be linked similarly based on their respective keys (e.g., GeographyKey,
CurrencyKey).
Understanding these relationships is crucial for any analysis involving multiple tables, as it allows for the combination of data across different aspects like customer demographics, product details, and sales transactions. For this implementation, Pyspark, Mathplotlib, Seaborn and Scikit-learn libraries were used.
DATA PRE-PROCESSING
Missing values were addressed by removing columns, such as "Title" and "MiddleName", that had more than seventy percent of their entries missing. The variables "BirthDate" and "DateFirstPurchase" were transformed to the date-time format. The categorical variables, such as MaritalStatus and Gender, were carefully maintained for consistency.
EXPLORATORY DATA ANALYSIS
The datasets were explored for insights and the following were some of the insights observed.
![Screen Shot 2024-06-18 at 13 46 48](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/d084c1af-afdc-4261-9766-8d4911f3bc90)
Figure 1: Monthly sales trend
Figure 1: Shows the monthly sales trends. This line chart provides insight into how sales have fluctuated over time, which is crucial for understanding seasonal patterns, identifying peak sales periods, and planning for inventory and marketing strategies.
![Screen Shot 2024-06-18 at 13 47 42](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/e4cccc9a-66c6-44bb-be74-48e16bfdde5b)
Figure 2: Sales distribution by product class
Figure 2: Shows the sales distribution by product class, the products in the medium class has a wider range but the high product class generate a higher average sales amount. This helps in understanding which product classes are generating higher revenue and how varied the sales figures are within each class. It's useful for product portfolio analysis and identifying which classes might need more marketing attention or pricing strategy adjustments.
![Screen Shot 2024-06-18 at 13 48 46](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/7f106b6d-e088-4d62-a3c6-99cc41bd317e)
Figure 3: Distribution of customer yearly income
Figure 3: Shows the distribution of customers' yearly incomes, with most customers earning between 20000 to 40000. This insight is valuable for understanding the economic segments of the customer base, which can inform targeted marketing strategies and product pricing.
![Screen Shot 2024-06-18 at 13 49 22](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/88676abf-ad02-41e1-8355-4cc02165c02c)
Figure 4: Top 10 popular products
Figure 4: Displays the top 10 popular products based on order quantity. It highlights which products are most frequently purchased, providing insights into customer preferences and potential inventory focus areas.
![Screen Shot 2024-06-18 at 13 51 02](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/cbd3dab5-b071-459d-8c5d-0c7c22171330)
Figure 5: Sales amount by marital status
Figure 5: Shows the average sales amount segmented by marital status, the single customers had a higher sales amount. This insight helps in understanding the purchasing power or behaviour of different marital groups, which can be valuable for targeted marketing campaigns.
![Screen Shot 2024-06-18 at 13 51 30](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/4c740638-fc60-488c-aa30-b039cfb5f711)
Figure 6: Sales amount by product colour
Figure 6: Visualizes sales amounts distributed across different product colors, silver products had higher
median sales amount. This insight can be used to identify which colors are more popular or generate higher revenue, informing product design and stock decisions.
![Screen Shot 2024-06-18 at 13 52 07](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/588f52b8-224f-4fc1-a5aa-36195326558a)
igure 7: Customer spending by number of cars owned
Figure 7: Shows the relationship between that customers with no cars were the highest average spenders. This can provide insights into the spending habits of customers based on their apparent wealth or lifestyle indicators.
![Screen Shot 2024-06-18 at 13 52 37](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/94fa4571-54e4-4931-8b43-a9e927857f34)
Figure 8: Sales amount by customer age group
Figure 8: categorizes sales amounts across different customer age groups. This insight helps to understand which age groups contribute most to the sales and how spending behavior varies with age. It's particularly useful for tailoring marketing strategies and product offerings to specific age demographics.
![Screen Shot 2024-06-18 at 13 53 22](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/6660e156-1836-49ca-961d-acd40e079220)
Figure 9: Geographic distribution of sales
Figure 9: Shows the total sales recorded in each country and the highest sales were recorded in the United
States.
![Screen Shot 2024-06-18 at 13 53 56](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/3d86dd47-4e46-4635-9152-0397b2032cda)
Figure 10: Sales impact by currency
Figure 10: Shows the total sales recorded in each country and the highest sales were recorded in the United
States.
![Screen Shot 2024-06-18 at 13 54 31](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/1aaff2e1-df8d-42f9-a07a-a5c98be4504f)
Figure 11: Sales volume by product price range
Figure 11: Shows the sales volumes of the different ranges of products and it can be observed that the low
range products have the highest sales volume.
CLASSIFICATION TASK: CUSTOMER SEGMENTATION
Classification models were used to complete a consumer segmentation requirement. The work made use of two datasets: DimCustomer and FactInternetSales. Following that, these datasets are loaded into Spark DataFrames. The DataFrames are linked by a common key, CustomerKey, which indicates a link between customer qualities and purchase behaviour.
It was done feature engineering to establish new columns TotalSales and PurchaseCount. These features total sales and count purchase orders for each consumer, giving you a complete picture of their spending patterns. Based on total sales, the segmentation logic splits clients into "Low-Value," "Medium-Value," and "High-Value" groups, with quantiles used as segmentation criteria. For model processing, categorical variables such as EnglishEducation and EnglishOccupation are translated into numerical indices
The data is used to train three classifiers: Random Forest, Decision Tree, and Naive Bayes. The performance of each model is evaluated based on accuracy, which is a measure of how often the model accurately predicts the client segment (Wei et al., 2019).
The Random Forest Classifier: achieved a high accuracy of 98.63%, indicating a high degree of confidence in the prediction of customer segments (low, medium, or high) for the test dataset, demonstrating the usefulness of this model for segmentation.
The Decision Tree Classifier: achieved an impressive 99.30% accuracy, which was the highest of the three models tested. This model was particularly effective at identifying consumer segments in the dataset.
The accuracy of the Naive Bayes Classifier, based on strong independent assumptions, was less effective at predicting consumer segments at 78.87%. The classification code snaphot is provided below.

![Screen Shot 2024-06-18 at 14 03 34](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/bac38eff-956e-4ebd-a546-f0b198be3089)
![Screen Shot 2024-06-18 at 14 04 01](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/c5aafcc7-9392-42cf-8782-3c0f2c45a0ea)
![Screen Shot 2024-06-18 at 14 04 29](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/7fc7dc2d-0682-4858-b568-517b250a08f4)

PREDICTION TASK: OPTIMAL PRICING PREDICTION
I used regression models to forecast product prices using DimProduct.cx, FactInternetSales.cx, and other data sources. I merged DataFrame on ProductKey for better insights. For the regression model, numerical features such as StandardCost and OrderQuantity were chosen. I split the data into 70 percent training and 30 percent testing sets. I created and trained 3 models (Linear regression, Decision tree, Gradient-boosted tree). I evaluated models on test set for predictions accuracy.

RMSE is used to evaluate model performance, the linear regression model has RMSE around 60.96 with an average prediction error around 60.96 currency units.
The decision tree regressor has RMSE around 39.94 showing higher accuracy with lower prediction error. Gradient-boosted tree regressor falls in between with RMSE around 56.54 bridging the difference between linear regression and decision tree model.
![Screen Shot 2024-06-18 at 14 13 52](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/c2b1aaa1-863d-4a67-bdd9-333e060378b4)
![Screen Shot 2024-06-18 at 14 14 54](https://github.com/AdedotunTemi/Big-Data-Processing-And-Analysis-Python/assets/168010102/362e6f00-df90-4e12-bb23-52333d09807f)

