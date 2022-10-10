var { readFileSync, writeFileSync } = require("fs");

console.log("UPDATING VERSION ...");

const packageJson = require("../package.json");
const newVersion = packageJson.version;
const files = ["versioning_function", "versioning_function_nochecks"];

files.forEach((fileName) => {
  const data = readFileSync(__dirname + `/../${fileName}.sql`, {
    encoding: "utf8",
    flag: "r",
  });

  const updated = data.replace(
    /-- version \d+.\d+.\d+/g,
    `-- version ${newVersion}`
  );

  writeFileSync(__dirname + `/../${fileName}.sql`, updated, {
    encoding: "utf8",
  });
});

console.log("VERSION UPDATED!");
