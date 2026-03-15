/**
 * api.js
 * All communication between the frontend and the FastAPI backend.
 */

const BASE_URL = "http://localhost:8000/api/v1";

/**
 * Upload files and start an assessment job.
 * @param {string} framework   e.g. "iso37001"
 * @param {File[]} files       Array of File objects from the drag-drop zone
 * @returns {Promise<{job_id: string}>}
 */
export async function startAssessment(framework, files) {
    const formData = new FormData();
    formData.append("framework", framework);
    files.forEach(file => formData.append("files", file));

    const response = await fetch(`${BASE_URL}/assess`, {
        method: "POST",
        body: formData,
    });

    if (!response.ok) {
        const err = await response.json();
        throw new Error(err.detail || "Failed to start assessment");
    }
    return response.json();
}

/**
 * Poll the backend for results until status == "completed".
 * @param {string} jobId
 * @param {function} onProgress   callback(statusText) for UI updates
 * @returns {Promise<object>}     the full assessment result payload
 */
export async function pollForResults(jobId, onProgress) {
    const POLL_INTERVAL_MS = 3000;
    const MAX_POLLS       = 120;   // 6 minutes max

    for (let i = 0; i < MAX_POLLS; i++) {
        await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));

        const response = await fetch(`${BASE_URL}/results/${jobId}`);
        if (!response.ok) throw new Error("Failed to fetch results");
        const data = await response.json();

        if (data.status === "completed") {
            return data;
        }

        if (data.status === "not_found") {
            throw new Error("Job not found on server. Please try again.");
        }

        if (onProgress) onProgress(`AI is analyzing your documents… (${(i + 1) * 3}s)`);
    }
    throw new Error("Assessment timed out. Please try again with fewer files.");
}
