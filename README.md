# Tarucca Cloud Engineer Intern - Technical Case Study

Hello!

I'm Hernani Costa, Co-founder of Tarucca. Thank you for your interest in joining our team.

You've made it to the final stage of our hiring process. This case study will help me understand how you approach real-world cloud engineering challenges - specifically the kind of work we do daily at Tarucca.

## About This Challenge

At Tarucca, we process time-series data from renewable energy IoT sensors. Currently, we run batch processing scripts manually. Your task is to demonstrate how you would modernize this into an event-driven, containerized pipeline.

**Important:** This case study simulates our AWS architecture using GitHub infrastructure (Actions + Docker). This allows you to demonstrate cloud engineering skills without AWS costs, while testing the exact same concepts we use in production.

## What You Need to Do

### Time Commitment
**Maximum 4 hours.** I value thoughtful implementation over perfection. If you hit the 4-hour mark, stop and document what you would do next.

### Deadline
**48 hours from receiving this package.** Work at your own pace, but submit within 2 days.

### Your Deliverables

1. **Fork this repository structure** to your GitHub account (instructions below)
2. **Complete the implementation** (details in each file's TODO sections)
3. **Make the pipeline work** end-to-end
4. **Record a short demo video** (3 minutes max)
5. **Submit your work** via email

---

## The Challenge: IoT Data Processing Pipeline

### Context
We receive CSV files from solar panel sensors throughout the day. Each file contains time-series data: voltage, current, temperature, and calculated power output.

Your job is to build an automated pipeline that:
- ✅ Triggers when new sensor data arrives
- ✅ Processes the data in a Docker container
- ✅ Calculates meaningful metrics (averages, peaks, anomalies)
- ✅ Outputs structured JSON results
- ✅ Runs automatically via CI/CD

### What I'm Providing

I've created a template repository structure with:
- `src/data_generator.py` - Generates realistic solar sensor data (complete, working)
- `src/processor.py` - Processing logic template with TODOs for you
- `tests/test_processor.py` - Test structure for you to complete
- `Dockerfile` - Container definition for you to complete
- `.github/workflows/pipeline.yml` - CI/CD workflow for you to complete
- Sample data files

### What You Need to Complete

#### 1. Processing Logic (`src/processor.py`)

Complete these three functions:

```python
def validate_data(record: dict) -> bool:
    """Check if sensor readings are within acceptable ranges"""
    # Implement validation logic
    
def calculate_metrics(data: List[dict]) -> Dict:
    """Calculate averages, min/max, standard deviation, energy totals"""
    # Implement metric calculations
    
def process_sensor_data(input_file: str, output_dir: str) -> dict:
    """Orchestrate: read → validate → calculate → save JSON"""
    # Implement main processing flow
```

**Expected output format:**
```json
{
  "input_file": "solar_data_20250115_140523.csv",
  "processed_at": "2025-01-15T14:07:45",
  "status": "success",
  "records_processed": 288,
  "records_invalid": 3,
  "metrics": {
    "voltage": {"avg": 25.2, "min": 23.1, "max": 27.8, "std": 1.4},
    "current": {"avg": 6.5, "min": 0.1, "max": 10.2},
    "temperature": {"avg": 32.5, "min": 20.0, "max": 45.0},
    "total_energy_kwh": 42.5,
    "peak_power_hour": "2025-01-15T12:00:00"
  }
}
```

#### 2. Dockerfile

Complete the Dockerfile to:
- Use Python 3.11-slim base image
- Install dependencies from requirements.txt
- Copy source code
- Set the working directory
- Define entrypoint to run the processor

#### 3. GitHub Actions Workflow (`.github/workflows/pipeline.yml`)

Complete the CI/CD pipeline to:
- Trigger when CSV files are added to `data/incoming/`
- Build the Docker image
- Run the processor container
- Upload processed results as artifacts
- Run tests with pytest

#### 4. Tests (`tests/test_processor.py`)

Write at least 3 meaningful tests:
- Valid data validation
- Invalid data detection
- Metric calculation accuracy

#### 5. Enhancement (Choose ONE)

Pick one to demonstrate depth:

**Option A - Error Handling:** Handle corrupt CSV files gracefully, output error logs

**Option B - Data Validation:** Detect anomalies (values outside expected ranges), flag suspicious readings

**Option C - Batch Processing:** Process multiple files in one run, generate summary report

**Option D - Monitoring Dashboard:** Generate HTML report showing daily statistics, charts (use any library)

---

## Setup Instructions

### Prerequisites

Make sure you have installed:
- Git
- Docker Desktop (running)
- Python 3.11 or higher
- A GitHub account

### Step 1: Create Your Repository

```bash
# Extract the ZIP file you received
unzip tarucca-case-study.zip
cd tarucca-cloud-engineer-case-study

# Initialize git repository
git init
git add .
git commit -m "Initial commit: Tarucca case study template"

# Create a new repository on GitHub (via web interface)
# Name it: tarucca-cloud-engineer-case-study
# Make it PUBLIC (so I can review it)

# Push your code
git remote add origin https://github.com/YOUR_USERNAME/tarucca-cloud-engineer-case-study.git
git branch -M main
git push -u origin main
```

### Step 2: Test Locally First

Before pushing to GitHub, test everything works on your machine:

```bash
# Generate sample data
python src/data_generator.py

# Install dependencies
pip install -r requirements.txt

# Test your processor locally
python src/processor.py

# Run tests
pytest tests/ -v

# Test Docker build
docker build -t tarucca-processor .

# Test Docker run
docker run -v $(pwd)/data:/app/data tarucca-processor

# Windows users use:
# docker run -v %cd%/data:/app/data tarucca-processor
```

### Step 3: Implement and Commit

Work iteratively. Make meaningful commits as you progress:

```bash
git add src/processor.py
git commit -m "Implement data validation logic"

git add Dockerfile
git commit -m "Complete Dockerfile with Python setup"

git add .github/workflows/pipeline.yml
git commit -m "Configure GitHub Actions for automated processing"
```

### Step 4: Trigger the Pipeline

Once your code is working locally:

```bash
# Generate a new data file
python src/data_generator.py

# Add and push the new CSV to trigger the pipeline
git add data/incoming/*.csv
git commit -m "Add sensor data to trigger pipeline"
git push
```

Go to your GitHub repository → Actions tab → Watch your pipeline run

### Step 5: Record Your Demo

Create a 3-minute video (use Loom, OBS, or phone screen recording) showing:

1. **Pipeline Running** (30 seconds)
   - Show GitHub Actions tab with successful run
   - Show the workflow steps executing

2. **Output Artifacts** (30 seconds)
   - Download artifacts from GitHub Actions
   - Open the processed JSON file
   - Show it contains correct metrics

3. **Architecture Explanation** (2 minutes)
   - Walk through your code structure
   - Explain key decisions (why you chose certain approaches)
   - Show your enhancement feature
   - Discuss what you'd do differently with more time

Upload to YouTube (unlisted) or Loom.

---

## Submission

Email me at: **hernani.costa@tarucca.com**

Subject: **"Tarucca Case Study Submission - [Your Name]"**

Include:
1. Link to your GitHub repository (make sure it's PUBLIC)
2. Link to your demo video
3. Brief notes (3-5 sentences):
   - What went well
   - What challenged you
   - What you'd improve given more time

---

## How I'll Evaluate Your Work

| Criteria | Weight | What I'm Looking For |
|----------|--------|---------------------|
| **Functionality** | 40% | Does the pipeline work end-to-end? Does it produce correct results? |
| **Code Quality** | 25% | Clean, readable Python. Proper error handling. Meaningful variable names. |
| **Docker & DevOps** | 20% | Dockerfile best practices. GitHub Actions configured correctly. |
| **Problem Solving** | 10% | How you approached challenges. Quality of your enhancement feature. |
| **Communication** | 5% | Clear commit messages. Good documentation. Articulate video explanation. |

### What I'm NOT Looking For

❌ Perfect, production-ready code (this is an internship test!)  
❌ Over-engineering or fancy frameworks  
❌ Spending 12 hours on this (respect the 4-hour limit)  

### What I AM Looking For

✅ Understanding of event-driven architecture  
✅ Practical Docker knowledge  
✅ Clean, understandable code  
✅ Ability to learn and figure things out  
✅ Good communication skills  

---

## FAQ

**Q: Can I use ChatGPT, Claude, or other AI tools?**  
A: Yes, absolutely. Modern developers use AI assistants. But you must understand and be able to explain every line of code you submit.

**Q: What if I get stuck?**  
A: Document what you tried and why it didn't work. Problem-solving approach matters as much as the solution.

**Q: Do I need AWS experience?**  
A: No. This case study intentionally uses GitHub + Docker to test the same concepts without AWS costs.

**Q: Can I add extra features beyond the requirements?**  
A: Only if you finish everything else first. Completing the core requirements well is better than partially implementing many features.

**Q: What if the pipeline doesn't run perfectly on GitHub Actions?**  
A: Show me it works locally (in your video) and explain what you'd debug. The learning process matters.

**Q: Can I ask questions?**  
A: Yes! Email me if something is genuinely unclear. But part of the test is figuring things out independently.

---

## Technical Hints

### For the Processor
- Use Python's `csv.DictReader` for parsing
- Use `statistics` module for calculations
- Use `pathlib.Path` for file operations
- Handle exceptions gracefully

### For Docker
- Keep the image small (use slim base images)
- Don't copy unnecessary files (.git, __pycache__)
- Use .dockerignore
- Set PYTHONUNBUFFERED=1 for proper logging

### For GitHub Actions
- Use `actions/checkout@v4`
- Use `actions/upload-artifact@v4`
- Test with `workflow_dispatch` trigger first (manual)
- Check the Actions logs if something fails

---

## About Tarucca

We're a renewable energy AI company working on optimization algorithms for solar/wind energy production. We work with consortium partners like TNO, Shell, and TU Delft.

This role will involve:
- Building data pipelines for sensor data
- Automating processing workflows
- Working with AWS (S3, Lambda, EC2)
- IoT device integration (Raspberry Pi, edge sensors)
- Real-time data processing

If you're excited about renewable energy, cloud engineering, and IoT - and this case study energizes you rather than stresses you - you'll fit right in.

---

## Final Notes

I designed this case study to mirror real work you'd do at Tarucca. It's challenging but achievable in 4 hours. 

Take your time. Think through the architecture. Write clean code. Document your decisions.

I'm looking forward to seeing what you build.

Good luck!

**Hernani Costa**  
Co-founder, Tarucca BV  
hernani.costa@tarucca.com

---

## Repository Structure

```
tarucca-cloud-engineer-case-study/
├── README.md                    ← You're reading this
├── data/
│   ├── incoming/                ← Raw sensor data goes here
│   │   ├── .gitkeep
│   │   └── sample_solar_data.csv
│   └── processed/               ← Processed JSON output goes here
│       └── .gitkeep
├── src/
│   ├── __init__.py
│   ├── data_generator.py        ← [COMPLETE] Generates test data
│   └── processor.py             ← [TODO] Your main work here
├── tests/
│   ├── __init__.py
│   └── test_processor.py        ← [TODO] Write tests
├── .github/
│   └── workflows/
│       └── pipeline.yml         ← [TODO] Complete CI/CD
├── .dockerignore
├── .gitignore
├── Dockerfile                   ← [TODO] Complete this
├── docker-compose.yml           ← [PROVIDED] For local testing
└── requirements.txt             ← [PROVIDED] Dependencies
```

Files marked [TODO] need your implementation.  
Files marked [PROVIDED] or [COMPLETE] are ready to use.
