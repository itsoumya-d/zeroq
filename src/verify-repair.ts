// Copyright (c) 2024-2026 Soumya Debnath. All Rights Reserved.
import { Message } from './types';

export interface RubricCheck {
  id: string;
  name: string;
  description: string;
  weight: number;
  mechanical: boolean;
}

export interface Rubric {
  name: string;
  version: number;
  passing_threshold: number;
  checks: RubricCheck[];
}

export interface VerificationViolation {
  check_id: string;
  name: string;
  weight: number;
  mechanical: boolean;
  reason: string;
}

export interface VerificationResult {
  score: number;
  passing_threshold: number;
  passed: boolean;
  violations: VerificationViolation[];
}

export interface RepairRoundLog {
  round: number;
  timestamp: number;
  score: number;
  status: 'passed' | 'repaired' | 'unresolved_non_mechanical';
  repairs?: string[];
  non_mechanical_violations?: VerificationViolation[];
}

export interface RepairResult {
  rounds: number;
  initial_score: number;
  final_score: number;
  passed: boolean;
  message: Partial<Message>;
  rounds_log: RepairRoundLog[];
  unresolved: VerificationViolation[];
}

export const DEFAULT_RUBRIC: Rubric = {
  name: 'zeroq-envelope-rubric',
  version: 1,
  passing_threshold: 85,
  checks: [
    {
      id: 'envelope_structure',
      name: 'Envelope Header and Required Fields',
      description: 'Verifies that envelope has non-empty id, topic, and numeric timestamp.',
      weight: 25,
      mechanical: true,
    },
    {
      id: 'topic_format',
      name: 'Topic Naming Convention',
      description: 'Topic must follow alphanumeric namespace format.',
      weight: 25,
      mechanical: false,
    },
    {
      id: 'payload_integrity',
      name: 'Payload Integrity',
      description: 'Payload must be defined and non-null.',
      weight: 25,
      mechanical: false,
    },
    {
      id: 'ttl_bounds',
      name: 'Time-To-Live (TTL) Validity',
      description: 'TTL must be a non-negative integer representing expiry in ms.',
      weight: 15,
      mechanical: true,
    },
    {
      id: 'header_types',
      name: 'Headers & Retry Metadata',
      description: 'retryCount must be non-negative integer and seq must be numeric.',
      weight: 10,
      mechanical: true,
    },
  ],
};

const TOPIC_REGEX = /^[a-zA-Z0-9_\-.]+$/;

export function verifyEnvelope(
  msg: Partial<Message>,
  rubric: Rubric = DEFAULT_RUBRIC
): VerificationResult {
  let score = 0;
  const violations: VerificationViolation[] = [];

  for (const check of rubric.checks) {
    let passed = false;
    let reason = '';

    if (check.id === 'envelope_structure') {
      const hasId = typeof msg.id === 'string' && msg.id.trim().length > 0;
      const hasTopic = typeof msg.topic === 'string' && msg.topic.trim().length > 0;
      const hasTs = typeof msg.timestamp === 'number' && msg.timestamp > 0;
      if (hasId && hasTopic && hasTs) {
        passed = true;
      } else {
        reason = 'Missing or invalid id, topic, or timestamp.';
      }
    } else if (check.id === 'topic_format') {
      const topic = String(msg.topic || '').trim();
      if (topic && TOPIC_REGEX.test(topic)) {
        passed = true;
      } else {
        reason = `Topic '${topic}' does not conform to valid namespace characters.`;
      }
    } else if (check.id === 'payload_integrity') {
      if (msg.payload !== undefined && msg.payload !== null) {
        passed = true;
      } else {
        reason = 'Payload is null or undefined.';
      }
    } else if (check.id === 'ttl_bounds') {
      const ttl = msg.ttl;
      if (typeof ttl === 'number' && ttl >= 0) {
        passed = true;
      } else {
        reason = `TTL '${ttl}' must be a non-negative integer.`;
      }
    } else if (check.id === 'header_types') {
      const retry = msg.retryCount;
      const seq = msg.seq;
      if (
        typeof retry === 'number' &&
        retry >= 0 &&
        typeof seq === 'number'
      ) {
        passed = true;
      } else {
        reason = 'retryCount and seq must be non-negative integers.';
      }
    }

    if (passed) {
      score += check.weight;
    } else {
      violations.push({
        check_id: check.id,
        name: check.name,
        weight: check.weight,
        mechanical: check.mechanical,
        reason,
      });
    }
  }

  return {
    score,
    passing_threshold: rubric.passing_threshold,
    passed: score >= rubric.passing_threshold,
    violations,
  };
}

export function repairEnvelope(
  msg: Partial<Message>,
  rubric: Rubric = DEFAULT_RUBRIC,
  maxRounds = 3
): RepairResult {
  const cur: Partial<Message> = JSON.parse(JSON.stringify(msg));
  const roundsLog: RepairRoundLog[] = [];

  for (let round = 1; round <= maxRounds; round++) {
    const v = verifyEnvelope(cur, rubric);
    const repairs: string[] = [];

    if (v.passed) {
      roundsLog.push({
        round,
        timestamp: Date.now(),
        score: v.score,
        status: 'passed',
        repairs,
      });
      break;
    }

    const mechanical = v.violations.filter((vio) => vio.mechanical);
    const nonMechanical = v.violations.filter((vio) => !vio.mechanical);

    if (mechanical.length === 0 && nonMechanical.length > 0) {
      roundsLog.push({
        round,
        timestamp: Date.now(),
        score: v.score,
        status: 'unresolved_non_mechanical',
        non_mechanical_violations: nonMechanical,
      });
      break;
    }

    for (const vio of mechanical) {
      if (vio.check_id === 'envelope_structure') {
        if (!cur.id) {
          cur.id = `zeroq_${Math.random().toString(36).slice(2, 11)}`;
          repairs.push('generated_missing_id');
        }
        if (!cur.timestamp || typeof cur.timestamp !== 'number' || cur.timestamp <= 0) {
          cur.timestamp = Date.now();
          repairs.push('set_current_timestamp');
        }
      } else if (vio.check_id === 'ttl_bounds') {
        cur.ttl = 60000;
        repairs.push('defaulted_missing_ttl');
      } else if (vio.check_id === 'header_types') {
        if (typeof cur.retryCount !== 'number' || cur.retryCount < 0) {
          cur.retryCount = 0;
          repairs.push('reset_negative_retryCount');
        }
        if (typeof cur.seq !== 'number') {
          cur.seq = 0;
          repairs.push('initialized_seq_to_zero');
        }
      }
    }

    roundsLog.push({
      round,
      timestamp: Date.now(),
      score: v.score,
      status: 'repaired',
      repairs,
    });
  }

  const finalV = verifyEnvelope(cur, rubric);
  return {
    rounds: roundsLog.length,
    initial_score: roundsLog.length > 0 ? roundsLog[0].score : finalV.score,
    final_score: finalV.score,
    passed: finalV.passed,
    message: cur,
    rounds_log: roundsLog,
    unresolved: finalV.violations,
  };
}
