// Copyright (c) 2024-2026 Soumya Debnath <soumyadebnath1661@gmail.com>. All rights reserved.
// Business Source License 1.1 (BSL 1.1) — Commercial License Key Validator

export interface LicenseValidationOptions {
  licenseKey?: string;
  allowEval?: boolean;
}

export class LicenseValidator {
  private static readonly AUTHOR = "Soumya Debnath";
  private static readonly CONTACT = "soumyadebnath1661@gmail.com";

  public static validate(options?: LicenseValidationOptions): boolean {
    const key = options?.licenseKey || (typeof process !== "undefined" ? process.env.COMMERCIAL_LICENSE_KEY : undefined);

    // Development / Localhost evaluation bypass
    const isDev = typeof window !== "undefined" 
      ? window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1"
      : typeof process !== "undefined" && process.env.NODE_ENV !== "production";

    if (isDev || options?.allowEval) {
      return true;
    }

    if (!key || !key.startsWith("BSL11-")) {
      console.warn(`
================================================================================
🔒 COMMERCIAL USE WARNING — BUSINESS SOURCE LICENSE 1.1 REQUIRED
Product: ZEROQ | Copyright (c) 2024-2026 Soumya Debnath

Production use of this software requires a valid paid commercial license key.
See LICENSE and COMMERCIAL_LICENSE.md for the applicable terms.

Purchase a commercial license key:
📧 soumyadebnath1661@gmail.com
   https://github.com/itsoumya-d/zeroq/blob/main/COMMERCIAL_LICENSE.md
================================================================================
      `);
      return false;
    }

    return true;
  }
}
