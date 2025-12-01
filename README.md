# Risk-analysis-chatbot

————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————
For the risk department of a financial company, types of risks involved include credit risk, counterparty risk, market risk, and others. Credit risk is primarily related to changes in the counterparty's credit, and the risk department needs to monitor daily changes in credit limiting. Counterparty risk and market risk are mainly associated with margin calls. Risk department must monitor the positions of counterparties daily, as well as price changes of the commodities they hold in the market, to determine whether margin calls are necessary. Additionally, there are many other risks specific to the company's business operations. A company may have numerous spreadsheets covering various aspects of its business. When someone unfamiliar with the company's operations, such as an intern, needs to find information or locate spreadsheets related to the company's business, they can only ask others or their mentors. Even for those who have been with the company for a long time, some details and rules can be easily forgotten.  

————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————
I have developed a conversational chatbot where employees can ask questions if they are unsure how to find information related to some information or the location of spreadsheets. They will first receive a brief response, followed by a complete answer. The current company setting is a futures company, and the questions involve commodity trading, trading permissions, the company's credit approval authority, and other related information.  

I first obtained an initial base spreadsheet which contains some sample indexed questions and their corresponding answers. The answers are divided into Answer 1 and Answer 2. Some questions include both Answer 1 and Answer 2, while others only have Answer 1. The answer types include text and tables. For example, when generating a trader's trading permissions, Answer 2 should output the corresponding codes based on the Chinese names of the commodities listed in Answer 1. Please help me translate this passage into English.

I utilized the TF-IDF algorithm to match user questions with predicted questions. For word segmentation, the Jieba Chinese text segmentation tool was employed. High weight should be assigned to a term that occurs with low frequency in the current document multiplied by its low frequency across the entire document collection. However, establishing distinct classes for different query types can significantly improve query performance.

The code defines multiple modules, including profit and loss query, account fund query, position query for a specific time, and Excel spreadsheet information extraction query. 

————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————
For **account profit and loss queries**, I have implemented daily, monthly, and yearly profit and loss calculations nested within the code. Users can input various time formats to query profit and loss for different time windows. In the Chinese language system, expressions such as '2025年10月5日' and '2025-10-05' are both commonly used. The system distinguishes between these formats, and if a user inputs the former, it automatically standardizes the time format through conversion. For profit and loss queries, users can also **query the latest day's profit and loss**. When a user's question contains the keywords *'latest'* and *'profit and loss'*, the relevant information can be retrieved.

————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————
The framework for **account fund queries** is almost identical to the framework for profit and loss queries. However, **account position queries** are relatively more complex because I need to locate the position of a specific commodity on a particular date, and users may phrase their questions in a wider variety of predicted patterns, requiring greater reliance on regular expressions. Additionally, `VALID_FUTURES` contains a list of common futures commodity names, primarily used to enhance robustness. After performing pattern matching to obtain the `variety` and `date`, it is best to verify whether the correct name has been extracted. For instance, the futures commodity "镍" (nickel) often gets incorrectly grouped with unrelated words like "在". In contrast, two-character commodity names are usually handled normally. Therefore, the purpose of `def _is_valid_future_variety` is to ensure that the commodity name has been accurately extracted.

————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————————








