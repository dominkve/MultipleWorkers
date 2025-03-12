// Function to submit a job
async function submitJob(code, mode, language, problem_id) {
    try {
        console.log("Submitting job...")
        const response = await fetch("http://127.0.0.1:5000/submit", {
            method: "POST",
            headers: { "Content-Type": "application/json"},
            body: JSON.stringify({ code, mode, language, problem_id })
        });

        if (!response.ok) {
            throw new Error("Failed to submit job");
        }

        const data = await response.json();
        console.log("Job submitted:", data);
        return data.job_id;
    } catch(error) {
        document.getElementById("output").textContent = "Error submitting a job: " + error.message;
    }
}

// Function to check job result
async function checkResult(job_id) {
    while (true) {
        try {
            console.log("Fetching results...")
            const response = await fetch("http://127.0.0.1:5000/result", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ job_id })
            });
            
            if (!response.ok) {
                throw new Error("Failed to fetch result");
            }

            const data = await response.json();
            console.log("Job result:", data);

            document.getElementById("output").textContent = `Status: ${data.status}\nOutput:\n${data.output}`;

            if (data.status !== "pending") {
                break;
            }

            await new Promise(resolve => setTimeout(resolve, 2000));
        } catch (error) {
            document.getElementById("output").textContent = "Error fetching result:" + error.message;
            break;
        }
    }
}

document.getElementById("codeForm").addEventListener("submit", async function(event) {
    event.preventDefault();
    
    let mode = document.getElementById("mode").value;
    let language = document.getElementById("language").value;
    let code = document.getElementById("codeInput").value;
    let problem_id = document.getElementById("problem_id").value;
    let submitButton = document.querySelector("input[type='submit']");

    console.log(problem_id)
    submitButton.disabled = true;
    document.getElementById("output").textContent = "Submitting job..."

    let job_id = await submitJob(code, mode, language, problem_id);

    if (job_id) {
        document.getElementById("output").textContent = "Job submitted. Waiting for result...";
        await checkResult(job_id);
    }
    
    submitButton.disabled = false;
});