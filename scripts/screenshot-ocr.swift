#!/usr/bin/swift

import Foundation
import AppKit
import Vision

struct OCRLine: Codable {
    let text: String
    let confidence: Double
}

struct OCRDocument: Codable {
    let image_path: String
    let lines: [OCRLine]
    let full_text: String
    let average_confidence: Double
}

func usage() {
    FileHandle.standardError.write(Data("Usage: swift scripts/screenshot-ocr.swift <image-path> [image-path...]\n".utf8))
}

func loadCGImage(from path: String) -> CGImage? {
    guard let image = NSImage(contentsOfFile: path) else { return nil }
    var rect = CGRect(origin: .zero, size: image.size)
    return image.cgImage(forProposedRect: &rect, context: nil, hints: nil)
}

func recognizeText(from path: String) throws -> OCRDocument {
    guard let cgImage = loadCGImage(from: path) else {
        throw NSError(domain: "screenshot-ocr", code: 1, userInfo: [NSLocalizedDescriptionKey: "Unable to load image: \(path)"])
    }

    var requestError: Error?
    var recognizedLines: [OCRLine] = []

    let request = VNRecognizeTextRequest { request, error in
        requestError = error
        guard error == nil else { return }
        let observations = request.results as? [VNRecognizedTextObservation] ?? []
        recognizedLines = observations.compactMap { observation in
            guard let candidate = observation.topCandidates(1).first else { return nil }
            return OCRLine(text: candidate.string, confidence: Double(candidate.confidence))
        }
    }
    request.recognitionLevel = .accurate
    request.usesLanguageCorrection = false
    request.minimumTextHeight = 0.015

    let handler = VNImageRequestHandler(cgImage: cgImage, options: [:])
    try handler.perform([request])

    if let requestError {
        throw requestError
    }

    let fullText = recognizedLines.map(\.text).joined(separator: "\n")
    let avgConfidence = recognizedLines.isEmpty
        ? 0.0
        : recognizedLines.map(\.confidence).reduce(0.0, +) / Double(recognizedLines.count)

    return OCRDocument(
        image_path: path,
        lines: recognizedLines,
        full_text: fullText,
        average_confidence: avgConfidence
    )
}

let args = CommandLine.arguments.dropFirst()
guard !args.isEmpty else {
    usage()
    exit(1)
}

var documents: [OCRDocument] = []

do {
    for path in args {
        documents.append(try recognizeText(from: path))
    }
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
    let data = try encoder.encode(documents)
    FileHandle.standardOutput.write(data)
    FileHandle.standardOutput.write(Data("\n".utf8))
} catch {
    FileHandle.standardError.write(Data("screenshot-ocr failed: \(error.localizedDescription)\n".utf8))
    exit(1)
}
