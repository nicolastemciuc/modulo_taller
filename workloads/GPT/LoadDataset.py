from datasets import Dataset
import re

SYSTEM_PROMPT = """<Título>
{title}
</Título>
<Contexto>
{context}
</Contexto>"""

def convert_to_prompt(title, context, question, tokenizer, answer=None):
    """
    Convert the given title, context, question, and answer into a formatted prompt, using the template defined in a tokenizer.
    
    Args:
        title (str): The title of the document.
        context (str): The context of the document.
        question (str): The question to be answered.
        tokenizer: The tokenizer used to format the prompt.
        answer (str): The answer to the question. If None, it will not be included in the prompt.
        
    Returns:
        str: The formatted prompt.
    """
    messages = [
        {'role': 'system', 'content': SYSTEM_PROMPT.format(title=title, context=context)},
        {'role': 'user', 'content': question}
    ]

    if answer:
        messages.append({'role': 'assistant', 'content': answer})

    prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=answer is None).replace("<|begin_of_text|>", "")

    # Remove Cutting Knowledge Date:...\nToday Date:...\n\n from the prompt
    prompt = re.sub(r'Cutting Knowledge Date:.*?\nToday Date:.*?\n\n', '', prompt, flags=re.DOTALL)
    return prompt

def convert_to_dict(title, context, question, tokenizer, answer):
    """
    Convert the given title, context, question, and answer into a formatted prompt, using the template defined in a tokenizer.
    
    Args:
        title (str): The title of the document.
        context (str): The context of the document.
        question (str): The question to be answered.
        tokenizer: The tokenizer used to format the prompt.
        answer (str): The answer to the question.
        
    Returns:
        str: The formatted prompt.
    """
    messages = [
        {'role': 'system', 'content': SYSTEM_PROMPT.format(title=title, context=context)},
        {'role': 'user', 'content': question},
    ]

    prompt = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True).replace("<|begin_of_text|>", "")

    # Remove Cutting Knowledge Date:...\nToday Date:...\n\n from the prompt
    prompt = re.sub(r'Cutting Knowledge Date:.*?\nToday Date:.*?\n\n', '', prompt, flags=re.DOTALL)

    return {"prompt": prompt,
            "completion": answer + tokenizer.eos_token}

def load_quales_train(path, tokenizer):
    """
    Load the dataset from the specified path and convert it into a list of formatted prompts.
    
    Args:
        path (str): The path to the dataset file.
        tokenizer: The tokenizer used to format the prompts.
        
    Returns:
        Dataset: A dataset containing the formatted prompts.
    """
    dataset = Dataset.from_json(path)
    
    texts = []
    for example in dataset:
        title = example["data"]["title"].strip()
        for paragraph in example["data"]["paragraphs"]:
            context = paragraph["context"].strip()
            for qa in paragraph["qas"]:
                question = qa["question"].strip()
                for answer in qa["answers"]:
                    if len(answer["text"]) > 0:
                        answer_text = answer["text"].strip()
                    else:
                        answer_text = "No responde"
                    texts.append(convert_to_dict(title, context, question, tokenizer, answer=answer_text))
    
    return Dataset.from_list(texts)

def load_quales_val(path, tokenizer):
    """
    Load the validation dataset from the specified path and convert it into a list of formatted prompts.
    
    Args:
        path (str): The path to the validation dataset file.
        tokenizer: The tokenizer used to format the prompts.
        
    Returns:
        Dataset: A dataset containing the formatted prompts.
    """
    dataset = Dataset.from_json(path)
    
    prompts = []
    answers = []
    for example in dataset:
        title = example["data"]["title"].strip()
        for paragraph in example["data"]["paragraphs"]:
            context = paragraph["context"].strip()
            for qa in paragraph["qas"]:
                question = qa["question"].strip()
                prompts.append(convert_to_prompt(title, context, question, tokenizer))
                answers.append([answer["text"] for answer in qa["answers"]])
    
    return Dataset.from_dict({"prompt": prompts, "answers": answers})