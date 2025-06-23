# 2024 Presidential Polling Dashboards

A comprehensive **data engineering and visualization project** analyzing 2024 presidential polling data using **Python**, **feature engineering**, and **Tableau**. This project demonstrates **machine learning engineering** skills through advanced data pipeline development, statistical analysis, and interactive dashboard creation.

## Project Overview

This **data science portfolio project** transforms raw polling data into interactive visualizations, featuring:

- **Automated data cleaning pipeline** with Python
- **Advanced feature engineering** for polling analysis
- **Statistical calculations** including margin of error and confidence intervals
- **Interactive Tableau dashboards** with filtering and time-series analysis

Stay tuned for:

- **Quality-weighted polling averages** and trend analysis

## Source Data

Data was available at FiveThirtyEight during the election season.  A copy of it, called `president_polls.csv`, is in the `data` directory.  Includes data from nearly 4,000 polls dating from April 7 2021 to November 4 2024.

## Usage Guide

### **Standard Processing**
```bash
cd processing-pipeline-files
python main.py ../data/raw/president_polls.csv
```

### **Debug Mode (Detailed Logging)**
```bash
python main.py ../data/raw/president_polls.csv --debug
```

### **Output**
- `../data/cleaned_polling_data.csv` - Analysis-ready dataset
- `polling_data_pipeline.log` - Processing logs and statistics

## Tableau Dashboards

[Link to `rolling-polling-trends.twbx` on Tableau Public:](https://public.tableau.com/app/profile/ashley.k.w.warren/viz/rolling-polling-trends/2024PresidentialRaceMulti-ResolutionPollingAnalysis?publish=yes).  Rolling averages for the candidates Trump, Biden, and Harris over time.  Toggle between populations sampled and geographic scopes.

## Future Enhancements

### **Advanced Analytics**
- [ ] **Machine learning models** for polling prediction
- [ ] **Sentiment analysis** integration from social media
- [ ] **Economic indicators** correlation analysis
- [ ] **Bayesian updating** for real-time forecasting

### **Enhanced Visualizations**
- [ ] **Geographic heat maps** for state-level analysis
- [ ] **Polling accuracy backtesting** with historical validation
- [ ] **Real-time data integration** via APIs
- [ ] **Mobile-responsive dashboards** for broader accessibility

### **Technical Improvements**
- [ ] **Database integration** for larger datasets
- [ ] **Automated data updates** with scheduling
- [ ] **Docker containerization** for deployment
- [ ] **API development** for data sharing

## Contact & Collaboration

**Interested in data science collaboration or have questions about this project?**

- **Email**: [ashleykwwarren@gmail.com]
- **LinkedIn**: [Ashley K. W. Warren](https://www.linkedin.com/in/ashleykwwarren/)

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Keywords

`data science` `machine learning engineering` `polling analysis` `python data pipeline` `tableau dashboard` `feature engineering` `statistical analysis` `data visualization` `presidential election` `political data` `data cleaning` `portfolio project` `interactive dashboard` `time series analysis` `polling methodology` `margin of error` `confidence intervals` `data quality` `electoral analysis`

---
