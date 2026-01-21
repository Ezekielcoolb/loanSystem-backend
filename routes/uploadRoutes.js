const express = require("express");
const multer = require("multer");
const fs = require("fs");
const path = require("path");

const router = express.Router();

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const tempPath = path.join(__dirname, "..", "uploads", "temp");
    fs.mkdirSync(tempPath, { recursive: true });
    cb(null, tempPath);
  },
  filename: (req, file, cb) => {
    const uniqueSuffix = Date.now() + "-" + Math.round(Math.random() * 1e9);
    const ext = path.extname(file.originalname);
    cb(null, file.fieldname + "-" + uniqueSuffix + ext);
  },
});

const upload = multer({ storage });

router.post(
  "/api/fileupload/:folderName",
  upload.single("file"),
  async (req, res) => {
    try {
      const folderName = req.params.folderName;
      const file = req.file;

      if (!file) {
        return res.status(400).json({ error: "No file uploaded" });
      }

      const uploadPath = path.join(__dirname, "..", "uploads", folderName);
      fs.mkdirSync(uploadPath, { recursive: true });

      const outputFilePath = path.join(uploadPath, file.filename);

      // Move file from temp to target folder without processing
      fs.renameSync(file.path, outputFilePath);

      const fileUrl = `/uploads/${folderName}/${file.filename}`;

      return res.status(200).json({
        message: "File uploaded successfully",
        data: fileUrl,
      });
    } catch (err) {
      console.error(err);
      return res.status(500).json({ error: "File upload failed" });
    }
  },
);

module.exports = router;
