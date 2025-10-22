import torch
import os
# from datasets import load_from_disk
from LoadDataset import load_quales_train
from transformers import AutoTokenizer, AutoModelForCausalLM, BitsAndBytesConfig
from trl import SFTConfig, SFTTrainer
from peft import LoraConfig
import torch.distributed as dist

# ---------------------- Distributed Training Setup ----------------------
rank = int(os.environ.get("RANK", 0))
local_rank = int(os.environ.get("LOCAL_RANK", 0))
world_size = int(os.environ.get("WORLD_SIZE", 1))

torch.cuda.set_device(local_rank)

dist.init_process_group(
    backend="nccl",
    init_method="env://",
    world_size=world_size,
    rank=rank,
    device_id=torch.device('cuda', 0)
)

# ------------------------------ Fine-Tuning Setup ------------------------
# Model and dataset paths
# MODEL_NAME = "meta-llama/Llama-3.2-1B-Instruct"
MODEL_NAME = "Qwen/Qwen2.5-0.5B-Instruct"
# NEW_MODEL_NAME = "Llama-3.2-1B-Instruct-lora-finetuned"
NEW_MODEL_NAME = "Qwen-2.5-0.5B-Instruct-lora-finetuned"
BASE_FOLDER = "base_folder/"
FOLDER = f'{BASE_FOLDER}outputs/{NEW_MODEL_NAME}'
DATASET = "dataset_covid_qa_train.json"

# Quantization Config
quant_config = BitsAndBytesConfig(
    load_in_4bit=True,
    bnb_4bit_quant_type="nf4",
    bnb_4bit_compute_dtype=torch.float16,
    bnb_4bit_use_double_quant=False
)
# Load Model and Tokenizer
model = AutoModelForCausalLM.from_pretrained(
    MODEL_NAME,
    quantization_config=quant_config,
    trust_remote_code=True
)

# ------------------------------ Tokenizer Setup -----------------------------
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
tokenizer.pad_token = tokenizer.eos_token
# LoRA Config
peft_parameters = LoraConfig(
    lora_alpha=32,
    lora_dropout=0.1,
    r=16,
    bias="none",
    task_type="CAUSAL_LM"
)

# # Load dataset
dataset = load_quales_train(DATASET, tokenizer)

# SFT Config
train_params = SFTConfig(
    output_dir=FOLDER,
    num_train_epochs=1,
    per_device_train_batch_size=1,
    gradient_accumulation_steps=1,
    optim="paged_adamw_32bit",
    save_steps=2000,
    logging_steps=25,
    learning_rate=2e-4,
    weight_decay=0.001,
    fp16=False,
    bf16=False,
    max_grad_norm=0.3,
    warmup_ratio=0.03,
    group_by_length=True,
    lr_scheduler_type="linear",
    report_to=None,
    gradient_checkpointing=False,
    dataset_text_field="prompt",
    packing = False
)


fine_tuning = SFTTrainer(
    model=model,
    train_dataset=dataset,
    peft_config=peft_parameters,
    args=train_params
)
# ------------------------------ Start Fine-Tuning ---------------------------

fine_tuning.train()

# Save Model
fine_tuning.model.save_pretrained(f'{BASE_FOLDER}/{NEW_MODEL_NAME}')
tokenizer.save_pretrained(f'{BASE_FOLDER}/{NEW_MODEL_NAME}')