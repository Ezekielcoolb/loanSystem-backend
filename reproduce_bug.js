const axios = require("axios");
const FormData = require("form-data");
const fs = require("fs");
const path = require("path");

const token =
  "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3ZjgzOWJiZDFkYzVlY2I0MGJmNDFiOSIsImVtYWlsIjoiY3NvNUBqa3NvbHV0bi5jb20iLCJpYXQiOjE3Njg5OTYzNDMsImV4cCI6MTc2OTYwMTE0M30.EjGUWMlpxQdbUq_er7Tg1v2zrivcdaChV6oKtF20H5o";
const API_URL = "http://localhost:5000/api/fileupload/customer";

async function testUpload() {
  try {
    const formData = new FormData();
    // Create a dummy file
    const dummyPath = path.join(__dirname, "dummy.txt");
    fs.writeFileSync(dummyPath, "test content");

    formData.append("file", fs.createReadStream(dummyPath));

    console.log("Sending request to:", API_URL);
    const response = await axios.post(API_URL, formData, {
      headers: {
        ...formData.getHeaders(),
        Authorization: `Bearer ${token}`,
      },
    });

    console.log("Response Status:", response.status);
    console.log("Response Data:", response.data);
  } catch (error) {
    console.error("Error Status:", error.response?.status);
    console.error("Error Data:", error.response?.data);
  }
}

testUpload();
