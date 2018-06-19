This project demonstrates the power of IBM cloud and computing capabilities along with the value added by the acquisition of The Weather Company.  Along with a concise report, this includes a live web app which predicts the travel times for patrons of CitiBike in NYC.  To provide this, a years worth of customer data was analyzed and combined with weather data.

1.  Scrape the bike data from the web and store it on a MySQL instance.  As the data has millions of rows, yet is still very structured, a relational database was best.  This also allowed easy integration of the weather data on the date and time keys.  This is done in 'get_bike_data.py'.


2.  Join all the data and export it back to CSV in a combined form.  This is done in 'data_retrieval.py'.  

3.  Filter outliers and clean the data for both analysis and visualization.  While the steps are very similar, there are enough small differences to warrant two suppurate functions within 'data_pipeline.py'.

4.  I then fed the visualization data into Watson Analytics and performed my visualizations there.

5.  For the analysis, I ran multiple cross validations with models ranging from support vector machines (rbf, linear, and poly kernels), LightGBM, Ridge, Lasso and TensorFlow neural nets.  The immense size of the data, however, required large sacrifices to be made for timely completion for the more complex algorithms.  Linear regression, on the other hand, was by far the fasted and could utilize the entire corpus of cleaned data.  Linear Regression ended up with the highest cross validated R2 scores and vastly superior training times.

6.  I then used job lib, the sklearn equivalent of pickle, to store the trained linear regression model.

7.  For new test data, I developed a web app which asks a few basic questions of the user such as age, gender and location to populate the necessary prediction features.  The starting station is guessed based on the IP address of the user and can be changed manually if wrong.  The weather data is also automatically pulled using the same methods as originally.  

8.  To run locally, create a virtual environment using python: 'python3 -m virtualenv [name]'.  Once the environment is activated with 'source [name]/bin/activate', install the requirements with 'pip install -r requirements.txt'.  From there, all it takes is running 'python welcome.py' to start the flask server.  Now navigating to 0.0.0.5000 will take you to the app.
# [Demo Web Application](https://erichensley-nlc-demo.mybluemix.net/)


