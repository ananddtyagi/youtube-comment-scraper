import json
import openai
from collections import defaultdict
import pandas as pd
from typing import Dict, List
import time

class CommentAnalyzer:
    def __init__(self, api_key: str):
        """Initialize with OpenAI API key"""
        openai.api_key = api_key
        self.analysis_template = """
        Analyze the following group of YouTube comments and identify:
        1. Main Topics: What are the most discussed subjects?
        2. Sentiment: What's the overall mood of the discussions?
        3. Common Issues: What problems or concerns are frequently mentioned?
        4. Notable Feedback: What constructive feedback or suggestions appear often?
        5. Community Engagement: How are people interacting with each other?

        Comments:
        {comments}

        Please structure your analysis and be specific about patterns you observe.
        """

    def chunk_comments(self, comments: List[str], max_tokens: int = 3000) -> List[List[str]]:
        """Split comments into chunks that fit within token limits"""
        chunks = []
        current_chunk = []
        current_length = 0
        
        # Rough estimate: 1 token ≈ 4 characters
        for comment in comments:
            comment_tokens = len(comment) // 4
            if current_length + comment_tokens > max_tokens:
                chunks.append(current_chunk)
                current_chunk = [comment]
                current_length = comment_tokens
            else:
                current_chunk.append(comment)
                current_length += comment_tokens
        
        if current_chunk:
            chunks.append(current_chunk)
        
        return chunks

    def analyze_chunk(self, comments: List[str]) -> str:
        """Analyze a single chunk of comments using the LLM"""
        try:
            response = openai.chat.completions.create(
                model="gpt-4o",  # or another appropriate model
                messages=[
                    {"role": "system", "content": "You are analyzing YouTube comments to identify patterns and insights."},
                    {"role": "user", "content": self.analysis_template.format(comments="\n".join(comments))}
                ],
                temperature=0.3
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"Error analyzing chunk: {str(e)}")
            return ""

    def merge_analyses(self, analyses: List[str]) -> str:
        """Merge multiple chunk analyses into a cohesive summary"""
        if not analyses:
            return "No analysis available."
        
        if len(analyses) == 1:
            return analyses[0]
        
        merge_prompt = """
        Combine these separate analyses into a single coherent summary,
        eliminating redundancies and highlighting the most significant patterns:

        {analyses}
        
        Provide a unified analysis that captures the key insights across all chunks.
        """
        
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are synthesizing multiple analyses into a coherent summary."},
                    {"role": "user", "content": merge_prompt.format(analyses="\n\n---\n\n".join(analyses))}
                ],
                temperature=0.3
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"Error merging analyses: {str(e)}")
            return "\n\n".join(analyses)

    def analyze_period(self, period_data: Dict) -> Dict:
        """Analyze all comments from a single period"""
        comments = period_data['comments']
        chunks = self.chunk_comments(comments)
        
        # Analyze each chunk
        chunk_analyses = []
        for i, chunk in enumerate(chunks, 1):
            print(f"Analyzing chunk {i}/{len(chunks)} ({len(chunk)} comments)")
            analysis = self.analyze_chunk(chunk)
            if analysis:
                chunk_analyses.append(analysis)
            time.sleep(1)  # Rate limiting
        
        # Merge analyses if there are multiple chunks
        final_analysis = self.merge_analyses(chunk_analyses)
        
        return {
            'period_metadata': {
                'comment_count': period_data['comment_count'],
                'video_count': period_data['video_count'],
                'unique_authors': period_data['unique_authors'],
                'date_range': period_data['date_range']
            },
            'analysis': final_analysis
        }

def main():
    # Configuration
    input_file = "grouped_comments.json"
    output_file = "comment_analysis.json"
    api_key = OPEN_AI_KEY
    
    # Load grouped comments
    with open(input_file, 'r', encoding='utf-8') as f:
        periods = json.load(f)
    
    # Initialize analyzer
    analyzer = CommentAnalyzer(api_key)
    
    # Analyze each period
    analyses = {}
    for period, data in periods.items():
        print(f"\nAnalyzing {period}")
        analyses[period] = analyzer.analyze_period(data)
    
    # Save results
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(analyses, f, indent=2, ensure_ascii=False)
    
    # Print summary
    print("\nAnalysis complete!")
    for period in analyses:
        print(f"\n{period}")
        print(f"Comments analyzed: {analyses[period]['period_metadata']['comment_count']}")
        print("Analysis length:", len(analyses[period]['analysis']))

if __name__ == "__main__":
    main()