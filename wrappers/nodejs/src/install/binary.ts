const supportedPlatforms: NodeJS.Platform[] = ["linux", "darwin"];
const supportedArchitectures: NodeJS.Architecture[] = ["x64", "arm64"];

export function checkPlatform(platform: NodeJS.Platform, arch: NodeJS.Architecture) {
  if (!supportedPlatforms.includes(platform)) {
    throw new Error(`Unsupported platform: ${platform}`);
  }
  if (!supportedArchitectures.includes(arch)) {
    throw new Error(`Unsupported architecture: ${arch}`);
  }
}
