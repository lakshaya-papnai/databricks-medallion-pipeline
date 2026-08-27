# AI Tool Workflow

## 1. Primary AI Tool
For this project, I used Cursor as my main AI assistant, specifically running Claude Sonnet 4.6. I chose Cursor because it actually understands the local workspace pretty well. Being able to keep the context across multiple chats and have it generate decent PySpark and SQL made it a solid pair-programming tool for building out the Medallion architecture.

## 2. Setting up Project Context (`.cursorrules`)
The main thing I kept coming back to was setting up a `.cursorrules` file in the project root. Since Cursor loads this automatically, it gave the AI the full project context without me having to copy-paste the same stuff over and over. 

I put a few key things in there: the full data schemas, the specific quality issues I planted (like the 50 NULL emails), and some hard architectural rules. For example, I was strict about Bronze never cleaning data, and Silver never deleting rows. I also threw in the DBFS paths and told it to only generate one file per prompt.

And that pretty much made sure the AI knew what was going on from the very first prompt.

## 3. Code Generation Approach
I tried to keep the code generation pretty controlled. I never asked the AI to dump out multiple files or a whole folder structure at once. Doing it one file at a time just kept things focused and made it way easier to review.

My prompts were usually pretty specific. I'd include the exact file name, what it needed to do, and the expected output, mostly just referencing the `.cursorrules` file so I didn't have to repeat the schemas. 

But I also had to push back on bad suggestions a lot. For example, in the Silver layer, the AI tried to use `dropDuplicates()` for the uniqueness checks and `INNER JOIN`s in the orchestrator. I rejected both since they violate the "never delete bad rows" rule, and told it to use `ROW_NUMBER()` and `LEFT JOIN`s instead.

## 4. Checking the AI's Code
Before actually saving and running the code, I made sure to check a few things. First, did it actually follow the rules I set? Like making sure it used `mode("overwrite")` instead of trying to delete files, and making sure it wasn't reading raw CSVs in the Gold layer.

I also caught a few logical errors. Like when the AI tried to use `NTILE(5)` instead of `PERCENT_RANK()` for the customer segmentation, or when it tried to sort weekly data just by `week_number` without considering the year. You really have to read the code.

## 5. Testing and Validation
Since I planted specific data issues, I just used those as my test cases. After a layer ran, I had the scripts read back from the Delta tables and print out summary metrics. Like in the Silver layer, I checked that exactly 50 orphan customer IDs were flagged. That proved the `LEFT JOIN` and `isNotNull()` logic was actually working.

## 6. Debugging
When things broke, I tried to actually look at the Spark logic instead of just blindly pasting stack traces back into the chat. 

For instance, when checking the total amounts, floating-point math caused a bunch of false positives. I just told the AI to throw a `spark_round()` in there before comparing. Another time, the AI created ambiguous column references by joining tables with identical column names. I had to tell it to use `.select()` to alias the columns properly before the join. 

Basically, I needed to make sure I understood the fix before applying it.

## 7. Data Privacy
I'm pretty strict about not sharing real stuff with AI. I didn't paste any real customer PII, credentials, or actual connection strings. Everything in this project is synthetically generated or mocked up. That was an intentional choice so I don't accidentally leak sensitive data to an LLM.

## 8. Lessons Learned
Overall, using Claude through Cursor definitely sped things up, especially for writing boilerplate PySpark and setting up the basic structure of the scripts.

But the whole process really highlighted that AI is just an assistant, not an architect. It kept defaulting to "easy" solutions that break good data engineering patterns, like trying to just filter out NULL rows instead of flagging them, or using an `INNER JOIN` when a `LEFT JOIN` was needed to prevent data loss.

It basically reinforced that while AI can write the syntax fine, you still have to own the architecture and review the code. Having that strict `.cursorrules` file and being willing to push back on bad suggestions was the only way to keep the Medallion architecture intact.
