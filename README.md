# Hotel Booking Data Analysis

##  Project Overview

This project analyzes hotel booking data using **Python and Pandas**.
The dataset contains information about booking dates, companies, guests, and room numbers. The goal of the analysis is to clean the dataset, handle missing values, and identify booking patterns from different booking platforms.

## Dataset

The dataset includes the following columns:

* **Date** – Booking date
* **Company** – Company associated with the booking
* **Person Name** – Guest name
* **Room number** – Assigned room number

Some rows contain **text markers such as booking platforms (Hotels, Booking, Expedia, Cleartrip)** instead of booking records. These rows require preprocessing before analysis.

## ⚙️ Technologies Used

* Python
* Pandas
* NumPy
* Matplotlib
* Jupyter Notebook

##  Data Cleaning Steps

The dataset required several preprocessing steps:

1. **Load dataset**

```python
import pandas as pd
import numpy as np

df = pd.read_csv("hotel-booking-data.txt", delimiter="\t")
```

2. **Handle missing values**

* Identify rows with missing room numbers
* Extract booking platform names

3. **Create new feature**

```python
df['2xRoom'] = df['Room number'] * 2
```

4. **Forward-fill booking platforms**

```python
mask = df['Room number'].isna()
df['Text Value'] = np.where(mask, df['Date'], np.nan)
df['Text Value'].fillna(method='bfill', inplace=True)
```

5. **Remove invalid rows**

```python
df.dropna(inplace=True)
```

##  Analysis Performed

### Platform Booking Distribution

The analysis shows the number of bookings coming from different platforms.

Example output:

| Platform         | Bookings |
| ---------------- | -------- |
| Expedia          | 48       |
| Hotels           | 39       |
| Booking          | 24       |
| Travel Agent 007 | 12       |
| Cleartrip        | 11       |

## 📊 Visualization

A simple visualization was created to compare booking platform counts.

```python
df3 = df.value_counts('Text Value')
df3.plot()
```

This helps identify which booking platform generates the most reservations.

##  Key Insights

* **Expedia generated the highest number of bookings**
* Multiple booking sources exist in the dataset
* Data preprocessing was necessary to extract meaningful information

##  How to Run the Project

1. Clone the repository

```bash
git clone https://github.com/ramubattu321/financial-data-analysis.git
```

2. Install dependencies

```bash
pip install pandas numpy matplotlib
```

3. Run the Jupyter Notebook

```bash
jupyter notebook
```

## 👤 Author

Ramu Battu
