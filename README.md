
# MapReduce Project: Analyzing Reviews and Extracting Top Terms

This MapReduce project processes a dataset of reviews, analyzes their terms and categories, and returns the top 150 terms per category based on their chi-squared (chi-sq) values, with the results alphabetically sorted. The implementation is in Java.


## **1. Visual Representation**
The schema provided is a visual representation of the processes and workflows involved in the project. It maps out each step, the corresponding MapReduce jobs, and the output at each stage.

---

![schema](https://github.com/user-attachments/assets/d7d70d8e-36bc-4f4f-8348-9e9983f142bb)

---

## **2. Shell Script for Running the Project**
The following shell script automates the process of removing old outputs, generating a jar file, and running the MapReduce jobs
---

## **3. Input Data**
- The input is a JSON file containing review data.

---

## **4. Step-by-Step Workflow**

### **2.1. Preprocessing and Term Filtering**
- **Mapper (`TermFilterMapper.java`):** Processes the input data by removing stopwords, handling delimiters, etc.  
  **Key:** `term;cat`  
  **Value:** `1`
  
- **Reducer (`TermFilterReducer.java`):** Aggregates the data, counting the occurrences of each term per category.  
  **Output:** A directory containing terms and their counts in each category:  
  **Key:** `term;cat`  
  **Value:** `n`

---

### **2.2. Count Reviews Per Category**
- **Mapper (`RevsPerCat_CounterMapper.java`):** Processes the input data to count the number of reviews per category.  
  **Key:** `category`  
  **Value:** `1`
  
- **Reducer (`RevsPerCat_CounterReducer.java`):** Aggregates the counts to calculate the total number of reviews per category.  
  **Output:** A directory with the review counts for each category:  
  **Key:** `category`  
  **Value:** `n`

---

### **2.3. Transform Data for Chi-Square Calculation**
- **Mapper (`PrepPerTermMapper.java`):** Prepares the data by transforming term counts into a per-term format:  
  **Key:** `term`  
  **Value:** `category;n`
  
- **Reducer (`PrepPerTermReducer.java`):** Consolidates the data, associating each term with all categories it appears in, along with their respective counts.  
  **Output:** A directory with terms prepared for chi-squared calculation:  
  **Format:** `term cat1:n,cat2:m,...`

---

### **2.4. Compute Chi-Squared Values**
- **Mapper (`ChiSquaredValuesMapper.java`):** Computes chi-squared values for each term-category pair.  
  **Key:** `term;category`  
  **Value:** `chi-sq`
  
- **Output Directory:** Contains chi-squared values:  
  **Format:** `term;cat chisq`

---

### **2.5. Extract Top 150 Terms Per Category**
- **Mapper (`Top150TermsMapper.java`):** Identifies the top 150 terms for each category based on chi-squared values.  
  **Key:** `category`  
  **Value:** `term;chisq`
  
- **Reducer (`Top150TermsReducer.java`):** Filters the data to retain only the top 150 terms per category.  
  **Output Directory:** Contains the top terms for each category:  
  **Format:** `category term1:chisq,term2:chisq,...`

---

### **2.6. Alphabetical Sorting**
- **Mapper (`AlphabeticalSortMapper.java`):** Reorganizes the key-value pairs for sorting:  
  **Key:** `1`  
  **Value:** `cat;term1:chisq,term2:chisq,...`
  
- **Reducer (`AlphabeticalSortedReducer.java`):** Orders the terms alphabetically for each category and outputs the sorted results.  
  **Output Directory:** Contains alphabetically sorted terms:  
  **Format:** `category term1:chisq,term2:chisq,...`

---

### **2.7. Merge and Final Output**
- The results are merged using `hdfs` commands into a final output file: `output.txt`.

**Final Format:**  
```
categoryA term1:chisq,term2:chisq,...
categoryB term1:chisq,term2:chisq,...
```

---


